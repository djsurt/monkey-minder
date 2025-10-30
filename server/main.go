package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"regexp"
	"strconv"
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
	var nodeId string
	flag.StringVar(&nodeId, "id", "", "The node id to use. This should match a name in the cluster config")
	flag.Parse()

	clusterMembers, err := parseClusterConfig("cluster.conf")
	if err != nil {
		fmt.Printf("Error reading cluster config: %v\n", err)
		os.Exit(1)
	}

	if _, ok := clusterMembers[nodeId]; !ok {
		fmt.Printf("Please provide a node id that is in the cluster config.\n")
		os.Exit(1)
	}

	myUrl := clusterMembers[nodeId]
	port, err := strconv.Atoi(myUrl.Port())
	if err != nil {
		fmt.Printf("Please specify a port number for this node in the cluster config")
		os.Exit(1)
	}

	// Filter myself from peers
	delete(clusterMembers, nodeId)

	electionServer := raft.NewElectionServer(port, clusterMembers)
	// Start the server
	fmt.Printf("Election server listening on port %d...\n", port)
	log.Fatal(electionServer.Serve())
}

func parseClusterConfig(configPath string) (peers map[string]url.URL, err error) {
	peers = make(map[string]url.URL)

	// Read from cluster config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	// Pares for valid peer entries
	configRegexp := regexp.MustCompile(`([[:alpha]]+)\s+(.*)\n`)
	matches := configRegexp.FindAllStringSubmatch(string(data), -1)

	// Loop over match, adding to peers map
	for _, match := range matches {
		id, rest := match[1], match[2]
		url, err := url.Parse(rest)
		if err != nil {
			return nil, err
		}
		peers[id] = *url
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
