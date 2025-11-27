package raft

import (
	"slices"
	"sync"

	raftpb "github.com/djsurt/monkey-minder/server/proto/raft"
)

type WatchPredicate func(committedEntry *raftpb.LogEntry) bool

type watch struct {
	predicate  WatchPredicate
	onComplete chan<- struct{}
}

type WatchManager struct {
	lock    sync.Mutex
	watches []watch
}

func NewWatchManager() *WatchManager {
	return &WatchManager{
		watches: make([]watch, 0),
	}
}

func (handler *WatchManager) AddWatch(predicate WatchPredicate) <-chan struct{} {
	handler.lock.Lock()
	defer handler.lock.Unlock()

	ch := make(chan struct{})
	handler.watches = append(handler.watches, watch{predicate: predicate, onComplete: ch})
	return ch
}

func (handler *WatchManager) SubmitEntry(committedEntry *raftpb.LogEntry) {
	handler.lock.Lock()
	defer handler.lock.Unlock()

	handler.watches = slices.DeleteFunc(
		handler.watches,
		func(w watch) bool {
			matched := w.predicate(committedEntry)
			if matched {
				w.onComplete <- struct{}{}
			}
			return matched
		},
	)
}
