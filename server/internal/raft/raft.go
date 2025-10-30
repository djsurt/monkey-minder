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
}

func NewElectionServer(port int) *ElectionServer {
	return &ElectionServer{
		Port:           port,
		state:          FOLLOWER,
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
	for {
		select {
		case aeReq := <-s.aeRequestChan:
			log.Printf("Follower recieved AppendEntries request from %v\n", aeReq.GetLeaderId())
			s.aeResponseChan <- &raftpb.AppendEntriesResult{
				Term:    int32(s.term),
				Success: true,
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
