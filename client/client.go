package client

import (
	"context"

	clientapi "github.com/djsurt/monkey-minder/common/proto"
	"google.golang.org/grpc"
)

type Version int64

type Client struct {
	ctx context.Context
	// TODO do we actually need to store this
	grpcConn *grpc.ClientConn
	// TODO do we actually need to store this
	grpcClient *clientapi.ApiClient
	session    grpc.BidiStreamingClient[clientapi.ClientRequest, clientapi.ServerResponse]
	idCounter  uint64
	chans      map[uint64]chan<- *clientapi.ServerResponse
}

// Create a new MonkeyMinder client that services all API calls to the
// MonkeyMinder server using a gRPC BIDI streaming client.
// Returns an error if the grpcClient fails to create a session.
func NewClient(ctx context.Context, target string) (client *Client, err error) {
	gprcConn, err := grpc.NewClient(target)
	if err != nil {
		return
	}

	grpcClient := clientapi.NewApiClient(gprcConn)

	session, err := grpcClient.Session(ctx)
	if err != nil {
		return
	}

	client = &Client{
		grpcConn:   gprcConn,
		grpcClient: &grpcClient,
		session:    session,
	}

	go client.handleResponses()

	return
}

// Helper to get the (monotonically increasing) id for the next client request.
func (client *Client) nextId() uint64 {
	client.idCounter++
	return client.idCounter
}

// Gets the next id if cond is true, 0 otherwise
func (client *Client) nextIdIf(cond bool) uint64 {
	if cond {
		return client.nextId()
	} else {
		return 0
	}
}

// this is a function instead of a method because golang hates me
func setupCallbackChannel[T any](
	client *Client,
	id uint64,
	convertResponse func(*clientapi.ServerResponse) T,
	outputChannel chan T,
) <-chan T {
	ch := make(chan *clientapi.ServerResponse)
	client.chans[id] = ch
	go func() { outputChannel <- convertResponse(<-ch) }()
	return outputChannel
}

// Response handler loop: handles responses from the server, routing incoming
// responses to the receive channel of the request that initiated the request.
func (client *Client) handleResponses() {
	for {
		resp, err := client.session.Recv()
		if err != nil {
			// TODO handle this properly
			panic(err)
		}
		if client.ctx.Err() != nil {
			return
		}

		ch, ok := client.chans[resp.Id]
		if ok {
			delete(client.chans, resp.Id)
			ch <- resp
		}
	}
}

// Sends a single MonkeyMinder request to the server.
func (client *Client) doApi(request *clientapi.ClientRequest) {
	err := client.session.Send(request)
	if err != nil {
		// TODO handle error properly
		panic(err)
	}
}

// Creates a node at the given path with the provided data.
func (client *Client) Create(path string, data string) <-chan string {
	request := &clientapi.ClientRequest{
		Kind: clientapi.RequestType_CREATE,
		Id:   client.nextId(),
		Path: &path,
		Data: &data,
	}
	onComplete := setupCallbackChannel(
		client,
		request.Id,
		func(resp *clientapi.ServerResponse) string { return *resp.Data },
		make(chan string),
	)
	go client.doApi(request)
	return onComplete
}

// Deletes the node at the given path if the node's version is equal to the
// provided version, or -1 if no version checking is required.
func (client *Client) Delete(path string, version Version) <-chan struct{} {
	request := &clientapi.ClientRequest{
		Kind:    clientapi.RequestType_DELETE,
		Id:      client.nextId(),
		Path:    &path,
		Version: int64(version),
	}
	onComplete := setupCallbackChannel(
		client,
		request.Id,
		func(*clientapi.ServerResponse) struct{} { return struct{}{} },
		make(chan struct{}),
	)
	go client.doApi(request)
	return onComplete
}

type NodeData struct {
	Data    string
	Version Version
}

func getData_convertResponse(resp *clientapi.ServerResponse) NodeData {
	return NodeData{
		Data:    *resp.Data,
		Version: Version(resp.Version),
	}
}

// Get the value of the node at path. On an update, the server promises to
// notify on the watchChan.
func (client *Client) GetData(path string, watchChan chan NodeData) <-chan NodeData {
	request := &clientapi.ClientRequest{
		Kind:    clientapi.RequestType_GETDATA,
		Id:      client.nextId(),
		WatchId: client.nextIdIf(watchChan != nil),
		Path:    &path,
	}
	onComplete := setupCallbackChannel(client, request.Id, getData_convertResponse, make(chan NodeData))
	if request.WatchId != 0 {
		setupCallbackChannel(client, request.WatchId, getData_convertResponse, watchChan)
	}
	go client.doApi(request)
	return onComplete
}

// Sets the node at the given path to data if the node's version is equal to
// the provided version, or -1 if no version checking is required.
func (client *Client) SetData(path string, data string, version Version) <-chan bool {
	request := &clientapi.ClientRequest{
		Kind:    clientapi.RequestType_GETDATA,
		Id:      client.nextId(),
		Path:    &path,
		Data:    &data,
		Version: int64(version),
	}
	onComplete := setupCallbackChannel(
		client,
		request.Id,
		func(sr *clientapi.ServerResponse) bool { panic("TODO") },
		make(chan bool),
	)
	go client.doApi(request)
	return onComplete
}

// Get the values of all children of the node at path. On an update, the
// server promises to notify on the watchChan.
func (client *Client) GetChildren(path string, watchChan <-chan []string) <-chan []string {
	request := &clientapi.ClientRequest{
		Kind: clientapi.RequestType_GETCHILDREN,
		Id:   client.nextId(),
		Path: &path,
	}
	onComplete := setupCallbackChannel(
		client,
		request.Id,
		func(sr *clientapi.ServerResponse) []string { return sr.Children },
		make(chan []string),
	)
	go client.doApi(request)
	return onComplete
}
