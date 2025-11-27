package main

import (
	"context"
	"log"
	"time"

	client "github.com/djsurt/monkey-minder/client/lib"
)

func main() {
	client, err := client.NewClient(context.Background(), "localhost:9001")
	if err != nil {
		panic(err)
	}

	for {
		currentFoo := <-client.GetData("/foo", nil)
		log.Printf("got value: %v", currentFoo)

		<- time.Tick(time.Second * 1)
	}
}
