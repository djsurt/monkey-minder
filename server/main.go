package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/djsurt/monkey-minder/server/internal/raft"
	raftpb "github.com/djsurt/monkey-minder/server/proto/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ElectionServer struct {
	raftpb.UnimplementedElectionServer
	clientID int
}

func main() {
	var port int
	flag.IntVar(&port, "port", -1, "The port number to use for this node")
	flag.Parse()

	// TODO: Check that the supplied port is in the cluster config.
	if port < 0 {
		fmt.Printf("Please provide a port number to bind to that is in the cluster file.\n")
		os.Exit(1)
	}

	peers, err := parseClusterConfig(port, "cluster.conf")
	if err != nil {
		fmt.Printf("Error reading cluster config: %v\n", err)
		os.Exit(1)
	}

	// Start the server in the background, listening for errors on the
	// serverErr channel
	electionServer := raft.NewElectionServer(port)
	serverErr := make(chan error)
	go func(errChan chan<- error) {
		fmt.Printf("Election server listening on port %d...\n", port)
		err := electionServer.Serve()
		if err != nil {
			errChan <- err
		}
	}(serverErr)

	// Start connecting to peers and sending messages.
	for _, peer := range peers {
		err := connectToPeer(port, peer)
		if err != nil {
			fmt.Printf("Error connectiong to peer %d: %v\n", peer, err)
		}
	}

	// Wait for an error from server
	err = <-serverErr
	fmt.Printf("Error from election server: %v\n", err)
	os.Exit(1)
}

func parseClusterConfig(myPort int, configPath string) (peers []int, err error) {
	bytes, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	for line := range strings.SplitSeq(string(bytes), "\n") {
		if line == "" {
			continue
		}
		peer, err := strconv.Atoi(line)
		if err != nil {
			fmt.Printf("Ignoring entry %s: could not parse to integer\n", line)
			continue
		}
		if peer != myPort {
			peers = append(peers, peer)
		}
	}
	return peers, nil
}

func connectToPeer(myPort int, peerPort int) error {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", peerPort), opts...)
	if err != nil {
		return err
	}
	client := raftpb.NewElectionClient(conn)
	heartbeat := &raftpb.AppendEntriesRequest{
		LeaderId: int32(myPort),
	}

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			<-ticker.C
			_, err := client.AppendEntries(context.TODO(), heartbeat)
			if err != nil {
				log.Printf("Error calling AppendEntries to %d: %v", peerPort, err)
				continue
			}
		}
	}()
	return nil
}
