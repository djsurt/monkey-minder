package raft

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/url"
	"sync"
	"time"

	raftpb "github.com/djsurt/monkey-minder/server/proto/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Term uint64

func (t Term) Raw() uint64 {
	return uint64(t)
}

type LogIndex uint64

func (i LogIndex) Raw() uint64 {
	return uint64(i)
}

func LogIndexFromRaw(raw uint64) LogIndex {
	return LogIndex(raw)
}

type NodeId uint64

type NodeState uint

const (
	FOLLOWER NodeState = iota
	CANDIDATE
	LEADER
)

type ElectionServer struct {
	raftpb.UnimplementedElectionServer
	Port           int
	peers          map[string]url.URL
	state          NodeState
	grpcServer     *grpc.Server
	listener       net.Conn
	peerConns      map[string]raftpb.ElectionClient
	term           Term
	logIndex       LogIndex
	aeRequestChan  chan *raftpb.AppendEntriesRequest
	aeResponseChan chan *raftpb.AppendEntriesResult
	rvRequestChan  chan *raftpb.VoteRequest
	rvResponseChan chan *raftpb.Vote
}

func NewElectionServer(port int, peers map[string]url.URL) *ElectionServer {
	return &ElectionServer{
		Port:           port,
		peers:          peers,
		state:          FOLLOWER,
		term:           1,
		logIndex:       1,
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

func (s *ElectionServer) doCommonAE(request *raftpb.AppendEntriesRequest) raftpb.AppendEntriesResult {
	panic("unimplemented")
}

func (s *ElectionServer) doCommonRV(request *raftpb.VoteRequest) raftpb.Vote {
	panic("unimplemented")
}

func (s *ElectionServer) doLeader(ctx context.Context) {
	// TODO: "Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts (§5.2)"
	// TODO: "If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)"
	// TODO: "If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex"
	//       "If successful: update nextIndex and matchIndex for follower (§5.3)"
	//       "If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)"
	// TODO: "If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)."

	// next entry to send
	var nextIndex map[NodeId]LogIndex = make(map[NodeId]LogIndex, len(s.peers))
	// highest entry known to be definitely on that peer
	var matchIndex map[NodeId]LogIndex = make(map[NodeId]LogIndex, len(s.peers))
	for id, _ := range s.peers {
		nextIndex[id] = s.log.IdxAfterLast()
		// initially, we don't know that any peers have anything,
		// so start at index before the first log entry
		matchIndex[id] = LogIndex(0)
	}

	doAESendChan := make(chan NodeId)

	setupPeerComm := func(id NodeId) (markDidAE func()) {
		// FIXME i think this will double-enqueue AEs in some situations?
		// TODO should be our election timeout (minus some epsilon?)
		heartbeatInterval := time.Second * 5
		shouldHeartbeat := time.NewTicker(heartbeatInterval)
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
		}
	}

	markPeerDidAE := make(map[NodeId]func(), len(s.peers))
	for id, _ := range s.peers {
		markPeerDidAE[id] = setupPeerComm(id)
	}

	for {
		select {
			// TODO also need to handle incoming stuff from clients
		case req := <-s.aeRequestChan:
			// incoming AppendEntries
			res := s.doCommonAE(req)
			s.aeResponseChan <- &res
			if s.term < Term(req.Term) {
				s.term = Term(req.Term)
				s.state = FOLLOWER
				return
			}
		case req := <-s.rvRequestChan:
			// incoming RequestVote
			res := s.doCommonRV(req)
			s.rvResponseChan <- &res
			if s.term < Term(req.Term) {
				s.term = Term(req.Term)
				s.state = FOLLOWER
				return
			}
		case id := <-doAESendChan:
			// outgoing AppendEntries
			markPeerDidAE[id]()
			panic("TODO")
		}
	}
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
				Term:    uint64(s.term),
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
	defer listener.Close()

	// Create & register gRPC server
	s.grpcServer = grpc.NewServer()
	raftpb.RegisterElectionServer(s.grpcServer, s)

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
func (s *ElectionServer) connectToPeers() error {
	// Set default DialOptions once
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	peerConns := make(map[string]raftpb.ElectionClient)
	for peer, url := range s.peers {
		conn, err := grpc.NewClient(url.String(), opts...)
		if err != nil {
			return err
		}
		client := raftpb.NewElectionClient(conn)
		peerConns[peer] = client
	}

	s.peerConns = peerConns
	return nil
}
