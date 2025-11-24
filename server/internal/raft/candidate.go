// This file contains the Candidate loop, logic for requesting votes from
// peers in parallel, and misc. types & helpers for processing the Candidate
// lifecycle.
package raft

import (
	"context"
	"log"

	raftpb "github.com/djsurt/monkey-minder/server/proto/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *RaftServer) doCandidate(ctx context.Context) {
	s.term += 1
	s.votedFor = s.Id
	votes := map[NodeId]struct{}{
		s.Id: {},
	}
	electionTimer := getNewElectionTimer()
	electionCtx, cancelElection := context.WithCancel(ctx)
	voteResponses := s.requestVotes(electionCtx)
	defer cancelElection()

	for {
		select {
		case vote := <-voteResponses:
			if vote.err != nil {
				switch status.Code(vote.err) {
				case codes.Unavailable:
					log.Printf("Error requesting vote from node %d: Node unavailable", vote.peer)
				default:
					log.Printf("Error requesting vote from node %d: %v", vote.peer, vote.err)
				}
			} else if s.term == vote.term && vote.granted {
				log.Printf("Vote received from node %d\n", vote.peer)
				log.Printf("Vote count: %d\n", len(votes))
				// Use set for counting votes to make sure repeated votes are idempotent.
				votes[vote.peer] = struct{}{}

				// Check for quorum
				if len(votes) > (len(s.peerConns)+1)/2 {
					log.Printf("Asserting myself as LEADER.\n")
					s.state = LEADER
					cancelElection()
					return
				}
			} else if vote.term > s.term {
				log.Printf("Received more recent term from node %d. Reverting to FOLLOWER...\n", vote.peer)
				s.term = vote.term
				s.state = FOLLOWER
				s.votedFor = 0
				return
			}
		case voteReq := <-s.rvRequestChan:
			// Reject votes unless the candidate has a higher term than me.
			vote, shouldAbdicate := s.doCommonRV(voteReq)
			if shouldAbdicate {
				log.Printf("Received vote request w/ more recent term from node %d. Reverting to FOLLOWER...\n", voteReq.CandidateId)
				s.state = FOLLOWER
				cancelElection()
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
				log.Printf("Received AppendEntries with higher term. Reverting to FOLLOWER...\n")
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
	// Do NOT close this channel, else it will cause doCandidate to spin for an
	// indeterminate amount of time.
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

	for peerId, peerConn := range s.peerConns {
		go func() {
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
		}()
	}

	return voteResponses
}
