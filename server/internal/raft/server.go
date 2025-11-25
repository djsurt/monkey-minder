// This file implements the Raft server and contains all methods for managing
// the lifecycle of the Raft consensus server.
package raft

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"time"

	raftlog "github.com/djsurt/monkey-minder/server/internal/log"
	tree "github.com/djsurt/monkey-minder/server/internal/tree"
	raftpb "github.com/djsurt/monkey-minder/server/proto/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Term uint64
type NodeId uint64

type NodeState uint

type Log = raftlog.Log[*raftpb.LogEntry, *tree.Tree]

const (
	FOLLOWER NodeState = iota
	CANDIDATE
	LEADER
)

type RaftServer struct {
	raftpb.UnimplementedRaftServer
	Port           int
	Id             NodeId
	peers          map[NodeId]string
	state          NodeState
	grpcServer     *grpc.Server
	listener       net.Conn
	peerConns      map[NodeId]raftpb.RaftClient
	term           Term
	votedFor       NodeId
	log            Log
	commitIdx      raftlog.Index
	tree           *tree.Tree
	aeRequestChan  chan *raftpb.AppendEntriesRequest
	aeResponseChan chan *raftpb.AppendEntriesResult
	rvRequestChan  chan *raftpb.VoteRequest
	rvResponseChan chan *raftpb.Vote
}

func NewRaftServer(port int, id NodeId, peers map[NodeId]string) *RaftServer {
	return &RaftServer{
		Port:  port,
		Id:    id,
		peers: peers,
		state: FOLLOWER,
		term:  1,
		// TODO should be loading from disk instead in the case where we do that
		log:            raftlog.NewLog(tree.NewTree(), 0),
		tree:           tree.NewTree(),
		aeRequestChan:  make(chan *raftpb.AppendEntriesRequest),
		aeResponseChan: make(chan *raftpb.AppendEntriesResult),
		rvRequestChan:  make(chan *raftpb.VoteRequest),
		rvResponseChan: make(chan *raftpb.Vote),
	}
}

// Run the state machine continuously, delegating to the appropriate node
// state handler loop.
func (s *RaftServer) doLoop(ctx context.Context) {
	for {
		switch s.state {
		case FOLLOWER:
			s.doFollower(ctx)
		case CANDIDATE:
			s.doCandidate(ctx)
		case LEADER:
			s.doLeader(ctx)
		}
	}
}

// Default election parameters. Change these to change election timeouts.
const (
	// Params recommended by Ongaro & Ousterhout, p. 14
	DEFAULT_MIN_TIMEOUT       int = 1500
	DEFAULT_MAX_TIMEOUT       int = 2000
	DEFAULT_HEARTBEAT_TIMEOUT int = DEFAULT_MIN_TIMEOUT / 2
)

// Helper function, returns a time channel that expires after a random
// election timeout
func getNewElectionTimer() <-chan time.Time {
	timeout_range := DEFAULT_MAX_TIMEOUT - DEFAULT_MIN_TIMEOUT
	dur := time.Duration(rand.Intn(timeout_range)+DEFAULT_MIN_TIMEOUT) * time.Millisecond
	return time.After(dur)
}

type ServerClosed struct{}

func (e *ServerClosed) Error() string {
	return "Server exited normally"
}

// Serve the ElectionServer RPC interface. Returns an error if any of the
// setup steps fail, or if the grpcServer returns an error due to a
// listener.accept() failure.
func (s *RaftServer) Serve() error {
	// Try to create the TCP socket.
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", s.Port))
	if err != nil {
		return fmt.Errorf("Error creating TCP socket: %v", err)
	}
	defer listener.Close()

	// Create & register gRPC server
	s.grpcServer = grpc.NewServer()
	raftpb.RegisterRaftServer(s.grpcServer, s)

	// Create peer connections
	err = s.connectToPeers()
	if err != nil {
		return err
	}

	// Start stateMachineLoop
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

// Connect to all peers for RPCs
func (s *RaftServer) connectToPeers() error {
	// Set default DialOptions once
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	peerConns := make(map[NodeId]raftpb.RaftClient)
	for peer, addr := range s.peers {
		conn, err := grpc.NewClient(addr, opts...)
		if err != nil {
			return err
		}
		client := raftpb.NewRaftClient(conn)
		peerConns[peer] = client
	}

	s.peerConns = peerConns
	return nil
}
