package raft

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
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
	log            Log
	aeRequestChan  chan *raftpb.AppendEntriesRequest
	aeResponseChan chan *raftpb.AppendEntriesResult
	rvRequestChan  chan *raftpb.VoteRequest
	rvResponseChan chan *raftpb.Vote
}

func NewElectionServer(port int, id NodeId, peers map[NodeId]string) *ElectionServer {
	return &ElectionServer{
		Port:  port,
		Id:    id,
		peers: peers,
		state: FOLLOWER,
		term:  1,
		// TODO should be loading from disk instead in the case where we do that
		log:            raftlog.NewLog(tree.NewTree(), 0),
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
	log.Printf("incoming AE: %v", request)

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

	lastEntry, lastIndex := s.log.GetEntryLatest()
	myLog := LastLog{Index: lastIndex}
	if lastEntry != nil {
		myLog.Term = Term((*lastEntry).Term)
	} else {
		myLog.Term = Term(0)
	}

	candidateLog := LastLog{
		Term:  Term(request.LastLogTerm),
		Index: raftlog.Index(request.LastLogIndex),
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
	// TODO: "If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)"
	// TODO: "If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)."

	// FIXME remove me
	testingAppendsTimer := time.NewTicker(5 * time.Second)

	rpcCtx, rpcCancel := context.WithCancel(ctx)
	defer rpcCancel()

	type leaderPeerData struct {
		// next entry to send
		nextIndex raftlog.Index
		// highest entry known to be definitely on that peer
		matchIndex   raftlog.Index
		markDidAE    func()
		markShouldAE func()
	}

	doAESendChan := make(chan NodeId)

	// FIXME need to plumb the ctx into this
	setupPeerComm := func(id NodeId) (markDidAE func(), markShouldAE func()) {
		// FIXME i think this will double-enqueue AEs in some situations?
		heartbeatInterval := time.Duration(DEFAULT_HEARTBEAT_TIMEOUT) * time.Millisecond
		// FIXME pretty sure we should actually make the initial initial timer duration zero?
		shouldHeartbeat := time.NewTicker(heartbeatInterval)
		// FIXME using `struct{}` a la unit type here feels pretty janky
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
			}, func() {
				go func() { shouldAE <- struct{}{} }()
			}
	}

	leaderPeers := make(map[NodeId]*leaderPeerData, len(s.peers))
	for id := range s.peers {
		markDidAE, markShouldAE := setupPeerComm(id)
		leaderPeers[id] = &leaderPeerData{
			nextIndex: s.log.IndexAfterLast(),
			// initially, we don't know that any peers have anything.
			matchIndex:   s.log.IndexBeforeFirst(),
			markDidAE:    markDidAE,
			markShouldAE: markShouldAE,
		}
	}

	type incomingAEResponse struct {
		result        *raftpb.AppendEntriesResult
		newNextIndex  raftlog.Index
		newMatchIndex raftlog.Index
		peer          NodeId
	}
	incomingAEResponses := make(chan incomingAEResponse)

	// "Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts (§5.2)"
	for _, lp := range leaderPeers {
		lp.markShouldAE()
	}

	for {
		select {
		// TODO also need to handle incoming stuff from clients
		case req := <-s.aeRequestChan:
			// incoming AppendEntries
			res, shouldAbdicate := s.doCommonAE(req)
			s.aeResponseChan <- res
			if shouldAbdicate {
				log.Printf("Abdicating to FOLLOWER.\n")
				s.state = FOLLOWER
				return
			}
		case req := <-s.rvRequestChan:
			// incoming RequestVote
			res, shouldAbdicate := s.doCommonRV(req, nil)
			s.rvResponseChan <- res
			if shouldAbdicate {
				log.Printf("Abdicating to FOLLOWER.\n")
				s.state = FOLLOWER
				return
			}
		case peerId := <-doAESendChan:
			// outgoing AppendEntries
			leaderPeers[peerId].markDidAE()

			var prevLogIndex, newNextIndex, newMatchIndex raftlog.Index
			var entriesToSend []*raftpb.LogEntry

			// "If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex"
			if s.log.IndexOfLast() >= leaderPeers[peerId].nextIndex {
				// inclusive
				toSendFirst := leaderPeers[peerId].nextIndex
				// exclusive
				toSendLast := s.log.IndexAfterLast()
				entriesToSend = make([]*raftpb.LogEntry, toSendLast-toSendFirst)
				for idx := toSendFirst; idx < toSendLast; idx++ {
					entry, err := s.log.GetEntryAt(idx)
					if err != nil {
						// this should never happen and if it does we've seriously messed up the index handling logic
						panic(err)
					}
					entriesToSend[idx-toSendFirst] = *entry
				}
				prevLogIndex = toSendFirst - 1
				newNextIndex = s.log.IndexAfterLast()
				newMatchIndex = s.log.IndexOfLast()
			} else {
				entriesToSend = make([]*raftpb.LogEntry, 0)
				prevLogIndex = s.log.IndexOfLast()
				newNextIndex = 0
				newMatchIndex = 0
			}

			prevLog, err := s.log.GetEntryAt(prevLogIndex)
			var prevLogTerm Term
			if err == nil {
				prevLogTerm = Term((*prevLog).Term)
			} else {
				// if we do snapshots this won't be correct
				if s.log.LenLogical() != s.log.LenActual() {
					panic("we seem to have started doing snapshots but this code was not updated to handle them correctly")
				}
				prevLogTerm = Term(0)
			}

			req := &raftpb.AppendEntriesRequest{
				Term:         uint64(s.term),
				LeaderId:     uint64(s.Id),
				PrevLogIndex: uint64(prevLogIndex),
				PrevLogTerm:  uint64(prevLogTerm),
				Entries:      entriesToSend,
			}

			go func(peerConn raftpb.ElectionClient, responses chan<- incomingAEResponse) {
				response, err := peerConn.AppendEntries(rpcCtx, req)
				if err != nil {
					return
				}
				responses <- incomingAEResponse{
					result:        response,
					newNextIndex:  newNextIndex,
					newMatchIndex: newMatchIndex,
					peer:          peerId,
				}
			}(s.peerConns[peerId], incomingAEResponses)
		case resp := <-incomingAEResponses:
			lp := leaderPeers[resp.peer]
			if resp.result.Success {
				// "If successful: update nextIndex and matchIndex for follower (§5.3)"
				lp.nextIndex = max(lp.nextIndex, resp.newNextIndex)
				lp.matchIndex = max(lp.matchIndex, resp.newMatchIndex)
			} else {
				// "If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)"
				// FIXME this is not distinguishing causes of failure, see above vs. what we have here
				lp.nextIndex--
				lp.markShouldAE()
			}
		case <-testingAppendsTimer.C:
			s.log.Append(&raftpb.LogEntry{
				Kind:       raftpb.LogEntryType_CREATE,
				Term:       uint64(s.term),
				TargetPath: "foo",
				Value:      "qux",
			})
		}
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
func (s *ElectionServer) requestVotes(ctx context.Context) <-chan VoteResult {
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

	go func() {
		var wg sync.WaitGroup
		for peerId, peerConn := range s.peerConns {
			wg.Go(func() {
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
			})
		}
		// Wait for all responses to be received
		wg.Wait()
		close(voteResponses)
	}()

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
