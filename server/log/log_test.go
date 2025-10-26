package log

import (
	"testing"
)

type simpleEntry struct {
	k string
	v int
}
type simpleSnapshot map[string]int

func simpleSquasher(oldSnapshot *simpleSnapshot, entries []simpleEntry) simpleSnapshot {
	var squashed simpleSnapshot
	if oldSnapshot == nil {
		squashed = simpleSnapshot{}
	} else {
		squashed = *oldSnapshot
	}
	for _, entry := range entries {
		squashed[entry.k] = entry.v
	}
	return squashed
}

func simpleQuerier(snapshot *simpleSnapshot, entries []simpleEntry, query string) int {
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].k == query {
			return entries[i].v
		}
	}
	if snapshot != nil {
		val, ok := (*snapshot)[query]
		if ok {
			return val
		}
	}
	return -1
}

func TestLog(t *testing.T) {
	log := NewEmptyLog(simpleSquasher, simpleQuerier)
	log.Append(simpleEntry{"a", 1})
	if log.Query("a") != 1 {
		t.Error()
	}
	log.Append(simpleEntry{"a", 2})
	if log.Query("a") != 2 {
		t.Error()
	}
	log.Append(simpleEntry{"b", 3})
	if log.Query("a") != 2 {
		t.Error()
	}
	log.SquashFirstN(1)
	if log.Query("a") != 2 {
		t.Error()
	}
}
