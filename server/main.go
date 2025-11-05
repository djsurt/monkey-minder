package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"regexp"
	"strconv"

	"github.com/djsurt/monkey-minder/server/internal/raft"
	raftpb "github.com/djsurt/monkey-minder/server/proto/raft"
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

	// Parse for valid peer entries
	configRegexp := regexp.MustCompile(`([[:alpha:]]+)\s+(.*)\n`)
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
