// This file contains the Candidate loop, logic for requesting votes from
// peers in parallel, and misc. types & helpers for processing the Candidate
// lifecycle.
package raft

import (
	"context"
	"log"
	"sync"

	raftpb "github.com/djsurt/monkey-minder/server/proto/raft"
)

func (s *RaftServer) doCandidate(ctx context.Context) {
	s.term += 1
	votedFor := s.Id
	voteCount := 1
	electionTimer := getNewElectionTimer()
	rpcCtx, rpcCancel := context.WithCancel(ctx)
	voteResponses := s.requestVotes(rpcCtx)
	defer rpcCancel()

	for {
		select {
		case vote := <-voteResponses:
			if vote.granted {
				log.Printf("Vote received from node %d\n", vote.peer)
				voteCount += 1

				// Check for quorum
				if voteCount > (len(s.peerConns)+1)/2 {
					log.Printf("Asserting myself as LEADER.\n")
					s.state = LEADER
					rpcCancel()
					return
				}
			} else {
				if vote.err != nil {
					log.Printf("Error requesting vote from node %d: %v", vote.peer, vote.err)
				}

				if vote.term > s.term {
					log.Printf("Received more recent term from node %d. Reverting to FOLLOWER...\n", vote.peer)
					s.term = vote.term
					s.state = FOLLOWER
					return
				}
			}
		case voteReq := <-s.rvRequestChan:
			// Reject votes because I've already voted for myself.
			vote, shouldAbdicate := s.doCommonRV(voteReq, &votedFor)
			if shouldAbdicate {
				log.Printf("Received vote request w/ more recent term from node %d. Reverting to FOLLOWER...\n", voteReq.CandidateId)
				s.state = FOLLOWER
				rpcCancel()
			}
			s.rvResponseChan <- vote
		case <-electionTimer:
			// Restart the Candidate loop
			s.state = CANDIDATE
			log.Printf("Election timed out. Restarting CANDIDATE state...")
			return
		case aeReq := <-s.aeRequestChan:
			res, shouldAbdicate := s.doCommonAE(aeReq)
			s.aeResponseChan <- res

			if shouldAbdicate {
				s.state = FOLLOWER
				return
			}
		}
	}
}

// Wrapper struct to hold the result and errors of a VoteRequest
type VoteResult struct {
	peer    NodeId
	granted bool
	term    Term
	err     error
}

// For each peer node, call RequestVote rpc in parallel. Results are sent on
// the VoteResult channel, which may be closed after the Candidate caller
// achieves quorum or abdicates.
func (s *RaftServer) requestVotes(ctx context.Context) <-chan VoteResult {
	voteResponses := make(chan VoteResult, len(s.peerConns))

	latestEntry, _ := s.log.GetEntryLatest()
	var lastLogTerm Term
	if latestEntry != nil {
		lastLogTerm = Term((*latestEntry).Term)
	} else {
		lastLogTerm = Term(0)
	}
	voteReq := &raftpb.VoteRequest{
		Term:         uint64(s.term),
		CandidateId:  uint64(s.Id),
		LastLogIndex: uint64(s.log.IndexOfLast()),
		LastLogTerm:  uint64(lastLogTerm),
	}

	go func() {
		var wg sync.WaitGroup
		for peerId, peerConn := range s.peerConns {
			wg.Go(func() {
				vote, err := peerConn.RequestVote(ctx, voteReq)

				voteResult := VoteResult{
					peer:    peerId,
					granted: vote.GetVoteGranted(),
					term:    Term(vote.GetTerm()),
					err:     err,
				}

				// Handle response appropriately
				select {
				case <-ctx.Done():
					return
				default:
					voteResponses <- voteResult
				}
			})
		}
		// Wait for all responses to be received
		wg.Wait()
		close(voteResponses)
	}()

	return voteResponses
}
