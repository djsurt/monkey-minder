package tree

import (
	"errors"
	"path"
	"strings"
)

type ZNode struct {
	Name     string
	Data     string
	Children map[string]*ZNode
}

type Tree struct {
	root *ZNode
}

func NewTree() *Tree {
	return &Tree{
		root: &ZNode{
			Name:     "",
			Children: make(map[string]*ZNode),
		},
	}
}

// Clean up the path to a standardied format
func normalize(p string) string {
	return path.Clean("/" + strings.TrimSpace(p))
}

// Create will insert a new node at the given path with data
func (t *Tree) Create(p string, data string) error {
	p = normalize(p)
	if p == "/" {
		return errors.New("cannot create root node")
	}
	parts := strings.Split(strings.TrimPrefix(p, "/"), "/")
	curr := t.root
	// Traverse through the tree to area where we need to insert
	for i, part := range parts {
		child, exists := curr.Children[part]
		if !exists {
			if i == len(parts)-1 {
				curr.Children[part] = &ZNode{
					Name:     part,
					Data:     data,
					Children: make(map[string]*ZNode),
				}
				return nil
			}
			return errors.New("parent does not exist: " + part)
		}
		curr = child
	}
	return errors.New("node already exists")
}

func (t *Tree) Get(p string) (string, error) {
	node := t.getNode(p)
	if node == nil {
		return "", errors.New("node not found")
	}
	return node.Data, nil
}

func (t *Tree) getNode(p string) *ZNode {
	p = normalize(p)
	if p == "/" {
		return t.root
	}
	parts := strings.Split(strings.TrimPrefix(p, "/"), "/")
	curr := t.root
	for _, part := range parts {
		child, exists := curr.Children[part]
		if !exists {
			return nil
		}
		curr = child
	}
	return curr
}

func (t *Tree) Update(p string, data string) error {
	node := t.getNode(p)
	if node == nil {
		return errors.New("node not found")
	}
	node.Data = data
	return nil
}

func (t *Tree) Delete(p string) error {
	p = normalize(p)
	if p == "/" {
		//TODO: We could allow deleting root node
		return errors.New("cannot delete root node")
	}

	parentPath := path.Dir(p)
	name := path.Base(p)

	parent := t.getNode(parentPath)
	if parent == nil {
		return errors.New("parent not found")
	}
	if _, ok := parent.Children[name]; !ok {
		return errors.New("node not found")
	}
	delete(parent.Children, name)
	return nil
}

// Deep copy of the tree
func (t *Tree) Clone() *Tree {
	return &Tree{
		root: cloneNode(t.root),
	}
}

// cloneNode recursively clones a node and all its children
func cloneNode(node *ZNode) *ZNode {
	if node == nil {
		return nil
	}

	// Create new node with copied data
	newNode := &ZNode{
		Name:     node.Name,
		Data:     node.Data,
		Children: make(map[string]*ZNode),
	}

	// Clone all the children recursively
	for key, child := range node.Children {
		newNode.Children[key] = cloneNode(child)
	}

	return newNode
}
