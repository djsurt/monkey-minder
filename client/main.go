package main

import (
	"context"

	client "github.com/djsurt/monkey-minder/client/lib"
)

func main() {
	client, err := client.NewClient(context.Background(), "localhost:9001")
	if err != nil {
		panic(err)
	}
	<-client.Create("foo", "bar")
}
