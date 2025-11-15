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
	"google.golang.org/grpc/credentials/insecure"
)

type Term uint64
type LogIndex uint64
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
	Id             NodeId
	peers          map[NodeId]string
	state          NodeState
	grpcServer     *grpc.Server
	listener       net.Conn
	peerConns      map[NodeId]raftpb.ElectionClient
	term           Term
	logIndex       LogIndex
	aeRequestChan  chan *raftpb.AppendEntriesRequest
	aeResponseChan chan *raftpb.AppendEntriesResult
	rvRequestChan  chan *raftpb.VoteRequest
	rvResponseChan chan *raftpb.Vote
}

func NewElectionServer(port int, id NodeId, peers map[NodeId]string) *ElectionServer {
	return &ElectionServer{
		Port:           port,
		Id:             id,
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

// Simple helper for updating term.
func (s *ElectionServer) updateTerm(newTerm Term) {
	s.term = newTerm
}

// Handle parts of AppendEntries request that are common to all node states.
// Returns an AppendEntriesResult and a boolean indicating whether the
// requestor's term is higher than the server's and should thus transition to
// follower.
func (s *ElectionServer) doCommonAE(request *raftpb.AppendEntriesRequest) (response *raftpb.AppendEntriesResult, staleTerm bool) {
	// §5.1: If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	staleTerm = Term(request.Term) > s.term
	if staleTerm {
		defer s.updateTerm(Term(request.Term))
	}

	response = &raftpb.AppendEntriesResult{
		Term: uint64(s.term),
	}

	// §5.1: Reply false if term < currentTerm
	if Term(request.Term) < s.term {
		response.Success = false
		return response, staleTerm
	}

	// TODO: Check that log[request.PrevLogIndex].Term == request.PrevLogTerm
	// §5.3: Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm

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
	Index LogIndex
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
// Mutates the value of votedFor when a vote is granted.
// Returns the Vote response and a boolean indicating whether the requestor's
// term is higher than the server's and should thus transition to follower.
func (s *ElectionServer) doCommonRV(request *raftpb.VoteRequest, votedFor *NodeId) (vote *raftpb.Vote, shouldAbdicate bool) {
	shouldAbdicate = Term(request.Term) > s.term
	if shouldAbdicate {
		defer s.updateTerm(Term(request.Term))
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

	myLog := LastLog{
		Term:  1,
		Index: s.logIndex,
	}
	candidateLog := LastLog{
		Term:  Term(request.LastLogTerm),
		Index: LogIndex(request.LastLogIndex),
	}

	// §5.2, §5.4: If votedFor is null or candidateId, and candidate's log
	// is at least as up-to-date as receiver's log, grant vote.
	if (votedFor == nil || *votedFor == NodeId(request.CandidateId)) &&
		candidateLog.AtLeastAsUpToDateAs(&myLog) {
		vote.VoteGranted = true
		votedFor = (*NodeId)(&request.CandidateId)
		log.Printf("Granting vote to CANDIDATE %d\n", request.CandidateId)
		return vote, shouldAbdicate
	}

	vote.VoteGranted = false
	return vote, shouldAbdicate
}

func (s *ElectionServer) doLeader(ctx context.Context) {
	// DEBUG: Just here to see that our leader election works.
	// Remove me later
	timer := time.After(30 * time.Second)

	rpcCtx, rpcCancel := context.WithCancel(ctx)
	defer rpcCancel()

	// Create a channel to receive heartbeat responses on
	responses := make(chan *raftpb.AppendEntriesResult)
	// Assert myself as leader
	s.sendHeartbeats(rpcCtx, responses)

	// periodically send heartbeats
	heartbeatTicker := time.NewTicker(500 * time.Millisecond)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-timer:
			log.Printf("Reverting to FOLLOWER...\n")
			s.state = FOLLOWER
			return
		case <-responses:
		case <-heartbeatTicker.C:
			s.sendHeartbeats(rpcCtx, responses)
		case aeReq := <-s.aeRequestChan:
			aeRes, shouldAbdicate := s.doCommonAE(aeReq)
			s.aeResponseChan <- aeRes
			if shouldAbdicate {
				log.Printf("Abdicating to FOLLOWER.\n")
				s.state = FOLLOWER
				return
			}
		}
	}
}

// Send AppendEntries heartbeats to all peers, asserting the caller as the
// elected leader.
func (s *ElectionServer) sendHeartbeats(ctx context.Context, responses chan<- *raftpb.AppendEntriesResult) {
	heartbeatRequest := &raftpb.AppendEntriesRequest{
		Term:         uint64(s.term),
		LeaderId:     uint64(s.Id),
		PrevLogIndex: uint64(s.logIndex), // TODO: Validate this is the correct value
		PrevLogTerm:  1,                  // TODO: use latest index from log
		Entries:      nil,
	}

	for _, peerConn := range s.peerConns {
		go func(responses chan<- *raftpb.AppendEntriesResult) {
			// log.Printf("Sending heartbeat to node %v\n", peerId)
			response, err := peerConn.AppendEntries(ctx, heartbeatRequest)
			if err != nil {
				// If the requests were cancelled, just need to terminate.
				//log.Printf("Error in AppendEntries RPC: %v", err)
				return
			}
			responses <- response
		}(responses)
	}
}

func (s *ElectionServer) doCandidate(ctx context.Context) {
	s.term += 1
	votedFor := s.Id
	voteCount := 1
	electionTimer := getNewElectionTimer()
	rpcCtx, rpcCancel := context.WithCancel(ctx)
	voteResponses := s.requestVotes(rpcCtx)
	defer rpcCancel()
	defer close(voteResponses)

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

type VoteResult struct {
	peer    NodeId
	granted bool
	term    Term
	err     error
}

// For each peer node, call RequestVote rpc in parallel. Results are sent on
// the VoteResult channel, which may be closed after the Candidate caller
// achieves quorum or abdicates.
func (s *ElectionServer) requestVotes(ctx context.Context) chan VoteResult {
	voteResponses := make(chan VoteResult, len(s.peerConns))

	voteReq := &raftpb.VoteRequest{
		Term:         uint64(s.term),
		CandidateId:  uint64(s.Id),
		LastLogIndex: uint64(s.logIndex),
		LastLogTerm:  1, // TODO: Use the latest log index from log module
	}

	for peerId, peerConn := range s.peerConns {
		go func(voteResult chan<- VoteResult) {
			vote, err := peerConn.RequestVote(ctx, voteReq)
			if err != nil {
				// If the requests were cancelled, just need to terminate.
				log.Printf("Error in RequestVotes RPC: %v", err)
				return
			}
			voteResult <- VoteResult{
				peer:    peerId,
				granted: vote.GetVoteGranted(),
				term:    Term(vote.GetTerm()),
				err:     err,
			}
		}(voteResponses)
	}

	return voteResponses
}

// Perform the follower loop, responding to RPC requests until an election
// timeout occurs. Set state to CANDIDATE and return upon an election timeout.
func (s *ElectionServer) doFollower(ctx context.Context) {
	electionTimer := getNewElectionTimer()
	var votedFor *NodeId

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
				votedFor = nil
			}
			electionTimer = getNewElectionTimer()

		case rvReq := <-s.rvRequestChan:
			vote, termChanged := s.doCommonRV(rvReq, votedFor)
			s.rvResponseChan <- vote
			if termChanged {
				votedFor = nil
			}
			electionTimer = getNewElectionTimer()
		}
	}
}

// Default election parameters. Change these to change election timeouts.
const (
	DEFAULT_MIN_TIMEOUT int = 1500
	DEFAULT_MAX_TIMEOUT int = 2000
)

// Helper function, returns a time channel that expires after a random
// election timeout
func getNewElectionTimer() <-chan time.Time {
	dur := time.Duration(rand.Intn(DEFAULT_MAX_TIMEOUT)+DEFAULT_MIN_TIMEOUT) * time.Millisecond
	return time.After(dur)
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
	//log.Printf("AppendEntries request received from %d", req.GetLeaderId())
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

	peerConns := make(map[NodeId]raftpb.ElectionClient)
	for peer, addr := range s.peers {
		conn, err := grpc.NewClient(addr, opts...)
		if err != nil {
			return err
		}
		client := raftpb.NewElectionClient(conn)
		peerConns[peer] = client
	}

	s.peerConns = peerConns
	return nil
}
