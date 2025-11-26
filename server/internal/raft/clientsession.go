package raft

import (
	"context"
	"fmt"
	"io"
	"log"

	clientapi "github.com/djsurt/monkey-minder/common/proto"
	"google.golang.org/grpc"
)

type clientSession struct {
	uid          uint64
	isLive       bool
	responseChan chan<- *clientapi.ServerResponse
}

func (s *RaftServer) Session(server grpc.BidiStreamingServer[clientapi.ClientRequest, clientapi.ServerResponse]) (err error) {
	log.Printf("got new incoming client session")

	ctx := server.Context()
	subCtx, cancel := context.WithCancel(ctx)

	responseChan := make(chan *clientapi.ServerResponse)
	requestChan := make(chan *clientapi.ClientRequest)

	// FIXME probably a better way to solve it than this
	recvError := make(chan error)

	sess := &clientSession{
		isLive:       true,
		responseChan: responseChan,
	}
	s.registerClientSession <- sess

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
		}
	}

	cancel()
	return
}

func (s *RaftServer) doRegisterClientSession(incomingSession *clientSession) {
	incomingSession.uid = s.clientSessNextUid
	s.clientSessNextUid++
	s.clientSessions = append(s.clientSessions, incomingSession)
}
