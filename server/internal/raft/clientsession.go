package raft

import (
	"context"
	"fmt"
	"io"
	"log"

	clientapi "github.com/djsurt/monkey-minder/common/proto"
	"github.com/djsurt/monkey-minder/server/internal/monkeyminder"
	"google.golang.org/grpc"
)

type sessionId uint64

type clientSession struct {
	uid          sessionId
	isLive       bool
	responseChan chan<- *clientapi.ServerResponse
}

type clientMsg struct {
	sessionId sessionId
	msg       monkeyminder.ClientMessage
}

func (s *RaftServer) Session(server grpc.BidiStreamingServer[clientapi.ClientRequest, clientapi.ServerResponse]) (err error) {
	log.Printf("got new incoming client session")

	ctx := server.Context()
	subCtx, cancel := context.WithCancel(ctx)

	defer cancel()

	responseChan := make(chan *clientapi.ServerResponse)
	requestChan := make(chan *clientapi.ClientRequest)

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

sendLoop:
	for {
		select {
		case <-subCtx.Done():
			break sendLoop
		case err = <-recvError:
			break sendLoop
		case request := <-requestChan:
			log.Printf("Received request: %v\n", request)
			s.clientMessagesIncoming <- clientMsg{
				sessionId: sess.uid,
				msg:       brokerMessage(request),
			}
		}
	}

	return
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
func brokerMessage(req *clientapi.ClientRequest) monkeyminder.ClientMessage {
	id := monkeyminder.SimpleMessageCommon{
		Id: monkeyminder.MessageId(req.Id),
	}
	watchId := monkeyminder.WatchMessageCommon{
		WatchId: monkeyminder.MessageId(req.WatchId),
	}

	var result monkeyminder.ClientMessage

	switch req.Kind {
	case clientapi.RequestType_CREATE:
		result = &monkeyminder.Create{
			SimpleMessageCommon: id,
			Path:                *req.Path,
			Data:                *req.Data,
		}
	case clientapi.RequestType_DELETE:
		result = &monkeyminder.Delete{
			SimpleMessageCommon: id,
			Path:                *req.Path,
			Version:             monkeyminder.Version(req.Version),
		}
	case clientapi.RequestType_EXISTS:
		result = &monkeyminder.Exists{
			SimpleMessageCommon: id,
			WatchMessageCommon:  watchId,
			Path:                *req.Path,
		}
	case clientapi.RequestType_GETDATA:
		result = &monkeyminder.GetData{
			SimpleMessageCommon: id,
			WatchMessageCommon:  watchId,
			Path:                *req.Path,
		}
	case clientapi.RequestType_SETDATA:
		result = &monkeyminder.SetData{
			SimpleMessageCommon: id,
			Path:                *req.Path,
			Data:                *req.Data,
			Version:             monkeyminder.Version(req.Version),
		}
	case clientapi.RequestType_GETCHILDREN:
		result = &monkeyminder.GetChildren{
			SimpleMessageCommon: id,
			WatchMessageCommon:  watchId,
			Path:                *req.Path,
		}
	case clientapi.RequestType_UNSPECIFIED:
		panic("Unspecified")
	}
	return result
}
