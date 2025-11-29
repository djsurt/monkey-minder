package raft

import (
	"log"
	"slices"
	"sync"

	raftlog "github.com/djsurt/monkey-minder/server/internal/log"
	raftpb "github.com/djsurt/monkey-minder/server/proto/raft"
)

type WatchPredicate func(committedEntry *raftpb.LogEntry, index raftlog.Index) bool

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

func (handler *WatchManager) SubmitEntry(committedEntry *raftpb.LogEntry, index raftlog.Index) {
	handler.lock.Lock()
	defer handler.lock.Unlock()

	log.Printf("checking watches for #%v (%v)", index, committedEntry)
	handler.watches = slices.DeleteFunc(
		handler.watches,
		func(w watch) bool {
			matched := w.predicate(committedEntry, index)
			if matched {
				w.onComplete <- struct{}{}
			}
			return matched
		},
	)
}
