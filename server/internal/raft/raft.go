package raft

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/url"
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
	peers          map[NodeId]url.URL
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
	votedFor       NodeId
}

func NewElectionServer(port int, id NodeId, peers map[NodeId]url.URL) *ElectionServer {
	return &ElectionServer{
		Port:           port,
		Id:             id,
		peers:          peers,
		state:          FOLLOWER,
		votedFor:       0,
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

func (s *ElectionServer) doCommonAE(request *raftpb.AppendEntriesRequest) raftpb.AppendEntriesResult {
	panic("unimplemented")
}

func (s *ElectionServer) doCommonRV(request *raftpb.VoteRequest) raftpb.Vote {
	panic("unimplemented")
}

func (s *ElectionServer) doLeader(ctx context.Context) {
	panic("unimplemented")
}

func (s *ElectionServer) doCandidate(ctx context.Context) {
	s.term += 1
	s.votedFor = s.Id
	voteCount := 1
	electionTimer := time.After(getNewElectionTimeout(150, 300))
	voteResponses := s.requestVotes(ctx)
	defer close(voteResponses)

	for {
		select {
		case vote := <-voteResponses:
			if vote.granted {
				log.Printf("Vote received from node %d\n", vote.peer)
				voteCount += 1

				// Check for quorum
				if voteCount > (len(s.peerConns)+1)/2 {
					log.Printf("Asserting myself as leader.\n")
					s.state = LEADER
					return
				}
			} else {
				if vote.err != nil {
					log.Printf("Error requesting vote from node %d: %v", vote.peer, vote.err)
				}

				if vote.term > s.term {
					log.Printf("Received more recent term from node %d. Reverting to follower...\n", vote.peer)
					s.term = vote.term
					s.state = FOLLOWER
					return
				}
			}
		case <-electionTimer:
			// Restart the Candidate loop
			s.state = CANDIDATE
			return
		case aeReq := <-s.aeRequestChan:
			peerTerm := Term(aeReq.GetTerm())
			peerId := NodeId(aeReq.GetLeaderId())

			// Another leader won
			if peerTerm >= s.term {
				log.Printf("Received an AppendEntries message from new leader %d. Reverting to follower...\n", peerId)
				// TODO: Handle AE request. For now, defering it to be handled by follower.
				s.aeRequestChan <- aeReq
				s.term = peerTerm
			} else {
				log.Printf("Rejecting AppendEntries from lower term node %d\n", peerId)
				// TODO: Refactor to use doCommonAE when its implemented
				s.aeResponseChan <- &raftpb.AppendEntriesResult{
					Term:    uint64(s.term),
					Success: false,
				}
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
				voteResult <- VoteResult{
					peer:    peerId,
					granted: false,
					term:    s.term,
					err:     err,
				}
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
	electionTimeout := time.After(getNewElectionTimeout(150, 300))
	for {
		select {
		case <-electionTimeout:
			log.Printf("Election timeout occurred. Switching to CANDIDATE state\n")
			s.state = CANDIDATE
			return
		case aeReq := <-s.aeRequestChan:
			resetTimeout := s.handleAppendEntriesAsFollower(aeReq)
			if resetTimeout {
				electionTimeout = time.After(getNewElectionTimeout(150, 300))
			}

		case rvReq := <-s.rvRequestChan:
			shouldResetTimeout := s.handleRequestVoteAsFollower(rvReq)
			if shouldResetTimeout {
				electionTimeout = time.After(getNewElectionTimeout(150, 300))
			}
		}
	}
}

func (s *ElectionServer) validateTerm(requestTerm Term, senderId uint64) bool {
	if requestTerm < s.term {
		log.Printf("Rejecting request from %v: term %d < current term %d\n",
			senderId, requestTerm, s.term)
		return false
	}
	return true
}

func (s *ElectionServer) updateTermIfNewer(requestTerm Term) {
	if requestTerm > s.term {
		log.Printf("Updating term from %d to %d\n", s.term, requestTerm)
		s.term = requestTerm
		s.votedFor = 0 // New term, can vote again
	}
}

func (s *ElectionServer) sendAppendEntriesResponse(success bool) {
	s.aeResponseChan <- &raftpb.AppendEntriesResult{
		Term:    uint64(s.term),
		Success: success,
	}
}

func (s *ElectionServer) canGrantVote(rvReq *raftpb.VoteRequest) bool {
	if s.votedFor != 0 && s.votedFor != NodeId(rvReq.GetCandidateId()) {
		log.Printf("Denying vote to %d: already voted for %d", rvReq.GetCandidateId(), s.votedFor)
		return false
	}
	candidateLogIndex := LogIndex(rvReq.GetLastLogIndex())
	if candidateLogIndex < s.logIndex {
		log.Printf("Denying vote to %d: candidate log not up-to-date", rvReq.GetCandidateId())
		return false
	}
	return true
}

func (s *ElectionServer) sendVoteResponse(success bool) {
	s.rvResponseChan <- &raftpb.Vote{
		Term:        uint64(s.term),
		VoteGranted: success,
	}
}

func (s *ElectionServer) validateLogConsistency(prevLogIndex LogIndex, prevLogTerm uint64) bool {
	// Simplified check for now - just verify we have entries up to prevLogIndex
	if prevLogIndex > s.logIndex {
		log.Printf("Rejecting: prevLogIndex %d > current logIndex %d\n",
			prevLogIndex, s.logIndex)
		return false
	}

	// TODO: When log is implemented, also check that term matches:
	// if s.log.GetEntry(prevLogIndex).Term != prevLogTerm {
	//     return false
	// }

	return true
}

func (s *ElectionServer) handleAppendEntriesAsFollower(aeReq *raftpb.AppendEntriesRequest) bool {
	log.Printf("Follower recieved AppendEntries request from %v\n", aeReq.GetLeaderId())
	// If the term in the request is less than our current term, return false
	requestTerm := Term(aeReq.GetTerm())

	if !s.validateTerm(requestTerm, aeReq.GetLeaderId()) {
		s.sendAppendEntriesResponse(false)
		return false // Don't reset timeout
	}

	s.updateTermIfNewer(requestTerm)

	//Check log consistency
	prevLogIndex := LogIndex(aeReq.GetPrevLogIndex())
	prevLogTerm := aeReq.GetPrevLogTerm()

	if !s.validateLogConsistency(prevLogIndex, prevLogTerm) {
		s.sendAppendEntriesResponse(false)
		return false // Don't reset timeout
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
	s.sendAppendEntriesResponse(true)
	return true // reset the timeout
}

func (s *ElectionServer) handleRequestVoteAsFollower(rvReq *raftpb.VoteRequest) bool {

	requestTerm := Term(rvReq.GetTerm())
	if !s.validateTerm(requestTerm, rvReq.GetCandidateId()) {
		s.sendVoteResponse(false)
		return false // Don't reset timeout
	}

	voteGranted := s.canGrantVote(rvReq)
	if voteGranted {
		s.votedFor = NodeId(rvReq.GetCandidateId())
		log.Printf("Granting vote to %d for term %d\n", rvReq.GetCandidateId(), rvReq.GetTerm())
	}

	s.sendVoteResponse(voteGranted)

	return voteGranted // reset the timeout if vote granted
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

	peerConns := make(map[NodeId]raftpb.ElectionClient)
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
