package raft

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	raftpb "github.com/djsurt/monkey-minder/server/proto/raft"
	"google.golang.org/grpc"
)

type NodeState uint

const (
	FOLLOWER NodeState = iota
	CANDIDATE
	LEADER
)

type ElectionServer struct {
	raftpb.UnimplementedElectionServer
	Port           int
	state          NodeState
	grpcServer     *grpc.Server
	listener       net.Conn
	term           uint
	logIndex       uint
	aeRequestChan  chan *raftpb.AppendEntriesRequest
	aeResponseChan chan *raftpb.AppendEntriesResult
	rvRequestChan  chan *raftpb.VoteRequest
	rvResponseChan chan *raftpb.Vote
	votedFor       *int
}

func NewElectionServer(port int) *ElectionServer {
	return &ElectionServer{
		Port:           port,
		state:          FOLLOWER,
		votedFor:       nil,
		aeRequestChan:  make(chan *raftpb.AppendEntriesRequest),
		aeResponseChan: make(chan *raftpb.AppendEntriesResult),
		rvRequestChan:  make(chan *raftpb.VoteRequest),
		rvResponseChan: make(chan *raftpb.Vote),
	}
}

// Run the state machine continuously, delegating to the appropriate node
// state handler loop.
func (s *ElectionServer) doLoop(ctx context.Context) {
	switch s.state {
	case FOLLOWER:
		s.doFollower(ctx)
	case CANDIDATE:
		s.doCandidate(ctx)
	case LEADER:
		s.doLeader(ctx)
	}
}

func (s *ElectionServer) doLeader(ctx context.Context) {
	panic("unimplemented")
}

func (s *ElectionServer) doCandidate(ctx context.Context) {
	panic("unimplemented")
}

// Perform the follower loop, responding to RPC requests until an election
// timeout occurs. Set state to CANDIDATE and return upon an election timeout.
func (s *ElectionServer) doFollower(ctx context.Context) {
	electionTimeout := time.After(getNewElectionTimeout(150, 300))
	for {
		select {
		case <-electionTimeout:
			log.Printf("Election timeout occurred. Switching to CANDIDATE state\n")
			s.state = CANDIDATE
			return
		case aeReq := <-s.aeRequestChan:
			log.Printf("Follower recieved AppendEntries request from %v\n", aeReq.GetLeaderId())
			// If the term in the request is less than our current term, return false
			if uint(aeReq.GetTerm()) < s.term {
				log.Printf("Rejecting AppendEntries request from %v: term %d < current term %d\n", aeReq.GetLeaderId(), aeReq.GetTerm(), s.term)
				s.aeResponseChan <- &raftpb.AppendEntriesResult{
					Term:    int32(s.term),
					Success: false,
				}
				continue
			}

			if uint(aeReq.GetTerm()) > s.term {
				s.term = uint(aeReq.GetTerm())
				s.votedFor = nil // New term, can vote again
			}
			// TODO: need to implement the bottom
			prev_log_index := uint(aeReq.GetPrevLogIndex())

			// Uncomment me when you're ready to implement log
			// prev_log_term := uint(aeReq.GetPrevLogTerm())

			// TODO: Since we don't have the log implemented, have a simple check for rejecting (replace with actual log logic eventually)
			//Placeholder logic for reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
			if prev_log_index > s.logIndex {
				log.Printf("Rejecting prevLogIndex %d > current logIndex %d\n", prev_log_index, s.logIndex)
				s.aeResponseChan <- &raftpb.AppendEntriesResult{
					Term:    int32(s.term),
					Success: false,
				}
				continue
			}

			//TODO: Delete conflicting entries
			// If an existing entry conflicts with a new one (same index, but dufferent terms)
			// delete the existing entry and all that follow it

			//TODO: Append new entries
			// for _, entry := range aeReq.GetEntries() {
			// 	s.log.Append(entry)
			// }

			//TODO: Update the commit index
			// if uint(aeReq.GetLeaderCommit()) > s.commitIndex {
			// 	s.commitIndex = min(uint(aeReq.GetLeaderCommit()), s.logIndex)
			// }
			electionTimeout = time.After(getNewElectionTimeout(150, 300))
			s.aeResponseChan <- &raftpb.AppendEntriesResult{
				Term:    int32(s.term),
				Success: true,
			}
		case rvReq := <-s.rvRequestChan:
			if uint(rvReq.GetTerm()) < s.term {
				log.Printf("Rejecting vote request from %v: term %d < current term %d\n", rvReq.GetCandidateId(), rvReq.GetTerm(), s.term)
				s.rvResponseChan <- &raftpb.Vote{
					Term:        int32(s.term),
					VoteGranted: false,
				}
				continue
			}
			voteGranted := false
			if s.votedFor == nil || *s.votedFor == int(rvReq.GetCandidateId()) {
				candidateLogIndex := uint(rvReq.GetLastLogIndex())
				if candidateLogIndex >= s.logIndex {
					voteGranted = true
					candidateId := int((rvReq.GetCandidateId()))
					s.votedFor = &candidateId
					electionTimeout = time.After(getNewElectionTimeout(150, 300))
					log.Printf("Granting vote to %d for term %d\n", rvReq.GetCandidateId(), rvReq.GetTerm())
				} else {
					log.Printf("Denying vote to %d: candidate log not up-to-date", rvReq.GetCandidateId())
				}
			} else {
				log.Printf("Denying vote to %d: already voted for %d", rvReq.GetCandidateId(), *s.votedFor)
			}
			s.rvResponseChan <- &raftpb.Vote{
				Term:        int32(s.term),
				VoteGranted: voteGranted,
			}
		}
	}
}

// Get a new election timeout value between min ms and max ms
func getNewElectionTimeout(min, max int) time.Duration {
	return time.Duration(rand.Intn(max)+min) * time.Millisecond
}

// Handle a RequestVote call from a peer in the candidate state.
func (s *ElectionServer) RequestVote(
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
func (s *ElectionServer) AppendEntries(
	ctx context.Context,
	req *raftpb.AppendEntriesRequest,
) (*raftpb.AppendEntriesResult, error) {
	log.Printf("AppendEntries request received from %d", req.GetLeaderId())
	// DO NOT MODIFY REQUEST after sending
	s.aeRequestChan <- req
	res := <-s.aeResponseChan
	return res, nil
}

type ServerClosed struct{}

func (e *ServerClosed) Error() string {
	return "Server exited normally"
}

// Serve the ElectionServer RPC interface. Returns an error if any of the
// setup steps fail, or if the grpcServer returns an error due to a
// listener.accept() failure.
func (s *ElectionServer) Serve() error {
	// Try to create the TCP socket.
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", s.Port))
	if err != nil {
		return fmt.Errorf("Error creating TCP socket: %v", err)
	}

	s.grpcServer = grpc.NewServer()
	raftpb.RegisterElectionServer(s.grpcServer, s)

	// start stateMachineLoop
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	go s.doLoop(ctx)

	// Begin serving ElectionServer RPCs
	err = s.grpcServer.Serve(listener)
	if err != nil {
		return err
	}

	return &ServerClosed{}
}
