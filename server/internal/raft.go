package raftpb

import (
	"context"
	"fmt"
	"log"
	"net"

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
	Port       int
	state      NodeState
	grpcServer *grpc.Server
	listener   net.Conn
}

func NewElectionServer(port int) *ElectionServer {
	return &ElectionServer{
		Port:  port,
		state: FOLLOWER,
	}
}

// Run the state machine
func (s *ElectionServer) doLoop(ctx context.Context) {
	switch s.state {
	case FOLLOWER:
		doFollower(ctx)
	case CANDIDATE:
		doCandidate(ctx)
	case LEADER:
		doLeader(ctx)
	}
}

func doLeader(ctx context.Context) {
	panic("unimplemented")
}

func doCandidate(ctx context.Context) {
	panic("unimplemented")
}

func doFollower(ctx context.Context) {
	panic("unimplemented")
}

// Handle a RequestVote call from a peer in the candidate state.
func (s *ElectionServer) RequestVote(
	ctx context.Context,
	req *raftpb.VoteRequest,
) (*raftpb.Vote, error) {
	log.Printf("Vote request received from %d", req.GetCandidateId())
	vote := &raftpb.Vote{Term: 1, VoteGranted: false}
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
	log.Printf("Heartbeat received from %d", req.GetLeaderId())
	res := &raftpb.AppendEntriesResult{
		Term:    req.GetTerm(),
		Success: true,
	}
	return res, nil
}

// Serve the ElectionServer RPC interface. Returns an error if any of the
// setup steps fail, or if the grpcServer returns an error due to a
// listener.accept() failure.
func (s *ElectionServer) Serve() (cancel context.CancelFunc, err error) {
	// Try to create the TCP socket.
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", s.Port))
	if err != nil {
		return nil, fmt.Errorf("Error creating TCP socket: %v", err)
	}

	s.grpcServer = grpc.NewServer()
	raftpb.RegisterElectionServer(s.grpcServer, s)

	// Begin serving ElectionServer RPCs
	err = s.grpcServer.Serve(listener)
	if err != nil {
		return nil, err
	}

	// start stateMachineLoop
	ctx, cancelFunc := context.WithCancel(context.Background())
	go s.doLoop(ctx)
	return cancelFunc, nil
}
