package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	monkeyminder "github.com/djsurt/monkey-minder"
)

func main() {
	id := flag.Int("id", 0, "The node id to use. This should match a name in the cluster config")
	flag.Parse()

	clusterMembers, err := parseClusterConfig("cluster.conf")
	if err != nil {
		fmt.Printf("Error reading cluster config: %v\n", err)
		os.Exit(1)
	}

	if _, ok := clusterMembers[*id]; !ok {
		fmt.Printf("Please provide a node id that is in the cluster config.\n")
		os.Exit(1)
	}

	myAddr := clusterMembers[*id]
	// Split host:port to get the port number
	parts := strings.Split(myAddr, ":")
	if len(parts) != 2 {
		fmt.Printf("Invalid address format: %s\n", myAddr)
		os.Exit(1)
	}

	client, err := monkeyminder.NewClient(context.Background(), myAddr)
	if err != nil {
		panic(err)
	}

	for range 1 {
		log.Println("Executing Create('/bar')...")
		if <-client.Create("/bar", "meow") {
			log.Println("Successfully created '/bar' w/ value 'meow'")
		} else {
			log.Println("Failed to create '/bar'")
		}
		<-time.After(5 * time.Second)

		log.Println("Executing GetData('/bar')...")
		bar := <-client.GetData("/bar", nil)
		log.Printf("Value of '/bar': %v\n", bar.Data)
		<-time.After(5 * time.Second)

		log.Println("Executing SetData('/bar', 'bark')...")
		<-client.SetData("/bar", "bark", -1)
		<-time.After(5 * time.Second)

		log.Println("Executing GetData('/bar')...")
		bar = <-client.GetData("/bar", nil)
		log.Printf("New value of /bar: %v\n", bar)
		<-time.After(5 * time.Second)

		log.Println("Executing Delete('/bar')...")
		<-client.Delete("/bar", -1)
		fmt.Println("Deleted /bar...")
		<-time.After(5 * time.Second)

		log.Println("Attempting to GetData('/bar')...")
		b := <-client.GetData("/bar", nil)
		log.Printf("/bar after delete: %v\n", b)
		<-time.After(5 * time.Second)
	}
}

func parseClusterConfig(configPath string) (peers map[int]string, err error) {
	peers = make(map[int]string)

	// Read from cluster config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	// Parse for valid peer entries
	configRegexp := regexp.MustCompile(`(\d+)\s+(.*)\n`)
	matches := configRegexp.FindAllStringSubmatch(string(data), -1)

	// Loop over match, adding to peers map
	for _, match := range matches {
		id_token, addr_token := match[1], match[2]
		// Convert id token to int
		id, err := strconv.Atoi(id_token)
		if err != nil {
			return nil, err
		}
		// Store the address string directly
		peers[id] = addr_token
	}
	return peers, nil
}
