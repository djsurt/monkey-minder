// This file contains Raft RPC implementations, common RPC processing logic,
// and any helper methods necessary to implement correct processing of RPC
// calls.
package raft

import (
	"context"
	"log"

	raftlog "github.com/djsurt/monkey-minder/server/internal/log"
	raftpb "github.com/djsurt/monkey-minder/server/proto/raft"
)

// Handle a RequestVote call from a peer in the candidate state.
func (s *RaftServer) RequestVote(
	ctx context.Context,
	req *raftpb.VoteRequest,
) (*raftpb.Vote, error) {
	log.Printf("Vote request received from %d", req.GetCandidateId())
	// DO NOT MODIFY REQUEST after sending
	s.rvRequestChan <- req
	vote := <-s.rvResponseChan
	return vote, nil
}

// When in the leader state, make an an AppendEntries either to update a
// follower's log, or to send a heartbeat to the follower.
// When in the follower state, respond to AppendEntries requests and udpate
// election timeout.
func (s *RaftServer) AppendEntries(
	ctx context.Context,
	req *raftpb.AppendEntriesRequest,
) (*raftpb.AppendEntriesResult, error) {
	//log.Printf("AppendEntries request received from %d", req.GetLeaderId())
	// DO NOT MODIFY REQUEST after sending
	s.aeRequestChan <- req
	res := <-s.aeResponseChan
	return res, nil
}

// Simple helper for updating term.
func (s *RaftServer) updateTerm(newTerm Term) {
	s.term = newTerm
}

// Handle parts of AppendEntries request that are common to all node states.
//
// Returns an AppendEntriesResult and a boolean indicating whether the
// requestor's term is greater than or equal to my term and should thus
// transition to FOLLOWER.
//
// Mutates s.votedFor and s.term when the AppendEntries' term is greater than
// my current term.
func (s *RaftServer) doCommonAE(request *raftpb.AppendEntriesRequest) (response *raftpb.AppendEntriesResult, staleTerm bool) {
	// §5.1: If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	staleTerm = Term(request.Term) > s.term
	if staleTerm {
		s.updateTerm(Term(request.Term))
		s.votedFor = 0
	}

	response = &raftpb.AppendEntriesResult{
		Term: uint64(s.term),
	}

	// §5.1: Reply false if term < currentTerm
	if Term(request.Term) < s.term {
		response.Success = false
		return response, staleTerm
	}

	// §5.3: Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	prevLogIdx := raftlog.Index(request.PrevLogIndex)
	prevLogEntry, err := s.log.GetEntryAt(prevLogIdx)
	// Follower's log doesn't match leader's
	if err != nil || (*prevLogEntry).Term != request.PrevLogTerm {
		response.Success = false
		return response, false
	}

	// TODO:
	// §5.3: If an existing entry conflicts with a new one (same index but
	// different terms), delete the existing entry and all that follow it

	// TODO:
	// Append any new entries not already in the log

	response.Success = true
	return response, staleTerm
}

// Used for comparing most recent logs during RequestVotes
type LastLog struct {
	Term  Term
	Index raftlog.Index
}

// Compare self with the other log. Returns true if self is at least as up
// to date as other. Returns false otherwise.
func (self *LastLog) AtLeastAsUpToDateAs(other *LastLog) bool {
	if self.Term == other.Term {
		return self.Index >= other.Index
	} else {
		return self.Term > other.Term
	}
}

// Handle a RequestVote request. Accepts the requestor's VoteRequest struct and
// a NodeId containing the value of the node the requestee voted for this cycle,
// which may be null.
//
// Mutates the value of votedFor when a vote is granted.
//
// If I am in CANDIDATE state and the request has a higher term than me,
// updates the s.term value to the candidate's term and grants them a vote.
//
// Returns the Vote response and a boolean indicating whether the requestor's
// term is higher than the server's and should thus transition to follower.
func (s *RaftServer) doCommonRV(request *raftpb.VoteRequest) (
	vote *raftpb.Vote, shouldAbdicate bool) {
	shouldAbdicate = Term(request.Term) > s.term
	// Change my vote if the candidate has a higher term than me.
	if shouldAbdicate {
		s.updateTerm(Term(request.Term))
		s.state = FOLLOWER
		s.votedFor = 0
	}

	vote = &raftpb.Vote{
		Term: uint64(s.term),
	}

	// §5.1: Reply false if term < currentTerm
	if Term(request.Term) < s.term {
		vote.VoteGranted = false
		return vote, shouldAbdicate
	}
	log.Printf("VOTE: My Term: %d, Candidate's Term: %d\n", s.term, request.Term)

	// Get the index and term numbers from the voter's last log entry.
	lastEntry, lastIndex := s.log.GetEntryLatest()
	myLog := LastLog{Index: lastIndex}
	if lastEntry != nil {
		myLog.Term = Term((*lastEntry).Term)
	} else {
		myLog.Term = Term(0)
	}

	// Get the index and term numbers from the candidate's last log entry.
	candidateLog := LastLog{
		Term:  Term(request.LastLogTerm),
		Index: raftlog.Index(request.LastLogIndex),
	}

	// §5.2, §5.4: If votedFor is null or candidateId, and candidate's log
	// is at least as up-to-date as receiver's log, grant vote.
	logOk := candidateLog.AtLeastAsUpToDateAs(&myLog)
	if Term(request.Term) == s.term && logOk &&
		(s.votedFor == 0 ||
			s.votedFor == NodeId(request.CandidateId)) {
		vote.VoteGranted = true
		s.votedFor = NodeId(request.CandidateId)
		log.Printf("Granting vote to CANDIDATE %d\n", request.CandidateId)
	} else {
		vote.VoteGranted = false
	}

	return vote, shouldAbdicate
}
