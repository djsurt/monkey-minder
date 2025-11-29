// This file contains the Follower loop and any useful utilities for performing
// the Follower lifecycle.
package raft

import (
	"context"
	"log"
)

// Perform the follower loop, responding to RPC requests until an election
// timeout occurs. Set state to CANDIDATE and return upon an election timeout.
func (s *RaftServer) doFollower(ctx context.Context) {
	electionTimer := getNewElectionTimer()

	for {
		select {
		case <-electionTimer:
			log.Printf("Election timeout occurred. Switching to CANDIDATE state\n")
			s.state = CANDIDATE
			return

		case aeReq := <-s.aeRequestChan:
			response, termChanged := s.doCommonAE(aeReq)
			s.aeResponseChan <- response
			if termChanged {
				s.votedFor = 0
			}
			electionTimer = getNewElectionTimer()

		case rvReq := <-s.rvRequestChan:
			vote, _ := s.doCommonRV(rvReq)
			s.rvResponseChan <- vote
			electionTimer = getNewElectionTimer()
		}
	}
}
