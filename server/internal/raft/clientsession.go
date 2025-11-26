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
	msg       *monkeyminder.ClientMessage
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
			s.clientMessages <- clientMsg{
				sessionId: sess.uid,
				msg:       panic("TODO: convert it"),
			}
		}
	}

	return
}
