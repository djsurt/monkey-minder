// This file contains the Leader loop and any utilites for implementing the
// Leader lifecycle.
package raft

import (
	"context"
	"log"
	"time"

	raftlog "github.com/djsurt/monkey-minder/server/internal/log"
	raftpb "github.com/djsurt/monkey-minder/server/proto/raft"
)

func (s *RaftServer) doLeader(ctx context.Context) {
	// TODO: "If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)"
	// TODO: "If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)."

	// FIXME remove me
	testingAppendsTimer := time.NewTicker(5 * time.Second)

	rpcCtx, rpcCancel := context.WithCancel(ctx)
	defer rpcCancel()

	type leaderPeerData struct {
		// next entry to send
		nextIndex raftlog.Index
		// highest entry known to be definitely on that peer
		matchIndex   raftlog.Index
		markDidAE    func()
		markShouldAE func()
	}

	doAESendChan := make(chan NodeId)

	// FIXME need to plumb the ctx into this
	setupPeerComm := func(id NodeId) (markDidAE func(), markShouldAE func()) {
		// FIXME i think this will double-enqueue AEs in some situations?
		heartbeatInterval := time.Duration(DEFAULT_HEARTBEAT_TIMEOUT) * time.Millisecond
		// FIXME pretty sure we should actually make the initial initial timer duration zero?
		shouldHeartbeat := time.NewTicker(heartbeatInterval)
		// FIXME using `struct{}` a la unit type here feels pretty janky
		shouldAE := make(chan struct{})
		// push auto heartbeats to the channel same channel as manual ones
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-shouldHeartbeat.C:
					shouldAE <- struct{}{}
				}
			}
		}()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-shouldAE:
					doAESendChan <- id
					shouldHeartbeat.Reset(heartbeatInterval)
				}
			}
		}()
		return func() {
				shouldHeartbeat.Reset(heartbeatInterval)
			}, func() {
				go func() { shouldAE <- struct{}{} }()
			}
	}

	leaderPeers := make(map[NodeId]*leaderPeerData, len(s.peers))
	for id := range s.peers {
		markDidAE, markShouldAE := setupPeerComm(id)
		leaderPeers[id] = &leaderPeerData{
			nextIndex: s.log.IndexAfterLast(),
			// initially, we don't know that any peers have anything.
			matchIndex:   s.log.IndexBeforeFirst(),
			markDidAE:    markDidAE,
			markShouldAE: markShouldAE,
		}
	}

	type incomingAEResponse struct {
		result        *raftpb.AppendEntriesResult
		newNextIndex  raftlog.Index
		newMatchIndex raftlog.Index
		peer          NodeId
	}
	incomingAEResponses := make(chan incomingAEResponse)

	// "Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts (§5.2)"
	for _, lp := range leaderPeers {
		lp.markShouldAE()
	}

	for {
		select {
		// TODO also need to handle incoming stuff from clients
		case req := <-s.aeRequestChan:
			// incoming AppendEntries
			res, shouldAbdicate := s.doCommonAE(req)
			s.aeResponseChan <- res
			if shouldAbdicate {
				log.Printf("Abdicating to FOLLOWER.\n")
				s.state = FOLLOWER
				return
			}
		case req := <-s.rvRequestChan:
			// incoming RequestVote
			res, shouldAbdicate := s.doCommonRV(req, nil)
			s.rvResponseChan <- res
			if shouldAbdicate {
				log.Printf("Abdicating to FOLLOWER.\n")
				s.state = FOLLOWER
				return
			}
		case peerId := <-doAESendChan:
			// outgoing AppendEntries
			leaderPeers[peerId].markDidAE()

			var prevLogIndex, newNextIndex, newMatchIndex raftlog.Index
			var entriesToSend []*raftpb.LogEntry

			// "If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex"
			if s.log.IndexOfLast() >= leaderPeers[peerId].nextIndex {
				// inclusive
				toSendFirst := leaderPeers[peerId].nextIndex
				// exclusive
				toSendLast := s.log.IndexAfterLast()
				entriesToSend = make([]*raftpb.LogEntry, toSendLast-toSendFirst)
				for idx := toSendFirst; idx < toSendLast; idx++ {
					entry, err := s.log.GetEntryAt(idx)
					if err != nil {
						// this should never happen and if it does we've seriously messed up the index handling logic
						log.Panicf("Log index logic error: %v", err)
					}
					entriesToSend[idx-toSendFirst] = *entry
				}
				prevLogIndex = toSendFirst - 1
				newNextIndex = s.log.IndexAfterLast()
				newMatchIndex = s.log.IndexOfLast()
			} else {
				entriesToSend = make([]*raftpb.LogEntry, 0)
				prevLogIndex = s.log.IndexOfLast()
				newNextIndex = 0
				newMatchIndex = 0
			}

			prevLog, err := s.log.GetEntryAt(prevLogIndex)
			var prevLogTerm Term
			if err == nil {
				prevLogTerm = Term((*prevLog).Term)
			} else {
				// if we do snapshots this won't be correct
				if s.log.LenLogical() != s.log.LenActual() {
					panic("we seem to have started doing snapshots but this code was not updated to handle them correctly")
				}
				prevLogTerm = Term(0)
			}

			req := &raftpb.AppendEntriesRequest{
				Term:         uint64(s.term),
				LeaderId:     uint64(s.Id),
				PrevLogIndex: uint64(prevLogIndex),
				PrevLogTerm:  uint64(prevLogTerm),
				Entries:      entriesToSend,
			}

			go func(peerConn raftpb.RaftClient, responses chan<- incomingAEResponse) {
				response, err := peerConn.AppendEntries(rpcCtx, req)
				if err != nil {
					return
				}
				responses <- incomingAEResponse{
					result:        response,
					newNextIndex:  newNextIndex,
					newMatchIndex: newMatchIndex,
					peer:          peerId,
				}
			}(s.peerConns[peerId], incomingAEResponses)
		case resp := <-incomingAEResponses:
			lp := leaderPeers[resp.peer]
			if resp.result.Success {
				// "If successful: update nextIndex and matchIndex for follower (§5.3)"
				lp.nextIndex = max(lp.nextIndex, resp.newNextIndex)
				lp.matchIndex = max(lp.matchIndex, resp.newMatchIndex)
			} else {
				// "If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)"
				// FIXME this is not distinguishing causes of failure, see above vs. what we have here
				lp.nextIndex--
				lp.markShouldAE()
			}
		case <-testingAppendsTimer.C:
			s.log.Append(&raftpb.LogEntry{
				Kind:       raftpb.LogEntryType_CREATE,
				Term:       uint64(s.term),
				TargetPath: "foo",
				Value:      "qux",
			})
		}
	}
}
