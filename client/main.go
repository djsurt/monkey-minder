package main

import (
	"context"

	"github.com/djsurt/monkey-minder/client/internal/client"
)

func main() {
	client, err := client.NewClient(context.Background(), "localhost:9001")
	if err != nil {
		panic(err)
	}
	<-client.Create("foo", "bar")
}
