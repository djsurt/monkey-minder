package raft

import (
	"context"
	"fmt"
	"io"
	"log"

	mmpb "github.com/djsurt/monkey-minder/proto"
	raftlog "github.com/djsurt/monkey-minder/server/internal/log"
	"github.com/djsurt/monkey-minder/server/internal/monkeyminder"
	raftpb "github.com/djsurt/monkey-minder/server/proto/raft"
	"google.golang.org/grpc"
)

type sessionId uint64

type clientSession struct {
	uid          sessionId
	isLive       bool
	responseChan chan<- *mmpb.ServerResponse
}

type clientMsg struct {
	sessionId sessionId
	msg       monkeyminder.ClientMessage
}

func (s *RaftServer) Session(server grpc.BidiStreamingServer[mmpb.ClientRequest, mmpb.ServerResponse]) (err error) {
	log.Printf("got new incoming client session")

	ctx := server.Context()
	subCtx, cancel := context.WithCancel(ctx)

	defer cancel()

	responseChan := make(chan *mmpb.ServerResponse)
	requestChan := make(chan *mmpb.ClientRequest)

	// FIXME probably a better way to solve it than this
	recvError := make(chan error)

	sess := &clientSession{
		uid:          sessionId(s.clientSessNextUid.Add(1)),
		isLive:       true,
		responseChan: responseChan,
	}
	s.clientSessions[sess.uid] = sess

	defer func() { sess.isLive = false }()

	go func() {
		for {
			select {
			case <-subCtx.Done():
				return
			default:
			}

			request, err := server.Recv()
			if err == io.EOF {
				recvError <- nil
				return
			} else if err != nil {
				recvError <- fmt.Errorf("error while receiving from client: %w", err)
				return
			} else {
				requestChan <- request
			}
		}
	}()

	go func() {
		for {
			select {
			case <-subCtx.Done():
				return
			case resp := <-responseChan:
				err := server.Send(resp)
				if err != nil {
					log.Printf("Error sending response to client: %v\n", err)
				}
			}
		}
	}()

clientLoop:
	for {
		select {
		case <-subCtx.Done():
			break clientLoop
		case err = <-recvError:
			break clientLoop
		case request := <-requestChan:
			log.Printf("Received request: %v\n", request)
			if request.Kind == mmpb.RequestType_INTERNAL_LEADERCHECK {
				responseChan <- &mmpb.ServerResponse{
					Id:               request.Id,
					Succeeded:        true,
					InternalIsleader: s.leader == s.Id,
					Version:          uint64(s.leader),
				}
			} else {
				s.clientMessagesIncoming <- clientMsg{
					sessionId: sess.uid,
					msg:       brokerMessage(request),
				}
			}
		}
	}

	return
}

func (s *RaftServer) handleClientMessage(msg clientMsg) {
	session := s.clientSessions[msg.sessionId]
	if msg.msg.IsLeaderOnly() {
		if s.state == LEADER {
			response, newEntries := msg.msg.DoMessage(*s.log.Latest())
			response.Id = uint64(msg.msg.GetId())
			for _, entry := range newEntries {
				entry.Term = uint64(s.term)
			}

			for _, entry := range newEntries {
				err := s.log.Append(entry)
				if err != nil {
					response.Succeeded = false
					if session.isLive {
						response.Data = nil
						session.responseChan <- response
					}
					return
				}
			}
			highestAwaitedIndex := s.log.IndexOfLast()
			consensusDone := s.watches.AddWatch(func(committedEntry *raftpb.LogEntry, index raftlog.Index) bool {
				return index >= highestAwaitedIndex
			})
			go func() {
				<-consensusDone
				if session.isLive {
					session.responseChan <- response
				}
				s.clientLeaderMessageDone <- struct{}{}
			}()
		} else {
			leaderClient := s.mmConns[s.leader]
			go func() {
				response := <-msg.msg.DoLeaderForward(leaderClient)
				log.Printf("RECEIVED RESPONSE FROM LEADER: %v\n", response)
				if session.isLive {
					session.responseChan <- response
				}
				s.clientLeaderMessageDone <- struct{}{}
			}()
		}
	} else {
		currentState := *s.log.Latest()
		response, newEntries := msg.msg.DoMessage(currentState)
		response.Id = uint64(msg.msg.GetId())
		if len(newEntries) > 0 {
			panic("non-LeaderOnly messages must not attempt to append log entries")
		}
		if session.isLive {
			session.responseChan <- response
		}
	}
}

// TODO give this a better name
func (s *RaftServer) clientMessageScheduler(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-s.clientMessagesIncoming:
			s.clientMessages <- msg
			// we need to not proceed to the next loop iteration until it is okay to execute a subsequent message
			if msg.msg.IsLeaderOnly() {
				<-s.clientLeaderMessageDone
			}
		}
	}
}

// Convert the incoming ClientRequest to the appropriate application message type
func brokerMessage(req *mmpb.ClientRequest) monkeyminder.ClientMessage {
	id := monkeyminder.SimpleMessageCommon{
		Id: monkeyminder.MessageId(req.Id),
	}
	watchId := monkeyminder.WatchMessageCommon{
		WatchId: monkeyminder.MessageId(req.WatchId),
	}

	var result monkeyminder.ClientMessage

	switch req.Kind {
	case mmpb.RequestType_CREATE:
		result = &monkeyminder.Create{
			SimpleMessageCommon: id,
			Path:                *req.Path,
			Data:                *req.Data,
		}
	case mmpb.RequestType_DELETE:
		result = &monkeyminder.Delete{
			SimpleMessageCommon: id,
			Path:                *req.Path,
			Version:             monkeyminder.Version(req.Version),
		}
	case mmpb.RequestType_EXISTS:
		result = &monkeyminder.Exists{
			SimpleMessageCommon: id,
			WatchMessageCommon:  watchId,
			Path:                *req.Path,
		}
	case mmpb.RequestType_GETDATA:
		result = &monkeyminder.GetData{
			SimpleMessageCommon: id,
			WatchMessageCommon:  watchId,
			Path:                *req.Path,
		}
	case mmpb.RequestType_SETDATA:
		result = &monkeyminder.SetData{
			SimpleMessageCommon: id,
			Path:                *req.Path,
			Data:                *req.Data,
			Version:             monkeyminder.Version(req.Version),
		}
	case mmpb.RequestType_GETCHILDREN:
		result = &monkeyminder.GetChildren{
			SimpleMessageCommon: id,
			WatchMessageCommon:  watchId,
			Path:                *req.Path,
		}
	case mmpb.RequestType_UNSPECIFIED:
		panic("Unspecified")
	}
	return result
}
