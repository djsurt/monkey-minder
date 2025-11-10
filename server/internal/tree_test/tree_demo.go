package main

import (
	"fmt"

	"github.com/djsurt/monkey-minder/server/internal/tree"
)

func main() {
	t := tree.NewTree()
	err := t.Create("/node1", "data1")
	fmt.Println("Create /node1:", err)

	data, err := t.Get("/node1")
	fmt.Println("Get /node1:", data, err)

	err = t.Update("/node1", "newData")
	fmt.Println("Update /node1:", err)

	data, err = t.Get("/node1")
	fmt.Println("Get updated /node1:", data, err)

	err = t.Delete("/node1")
	fmt.Println("Delete /node1:", err)
}
