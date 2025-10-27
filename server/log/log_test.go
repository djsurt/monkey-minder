package log

import (
	"maps"
	"testing"
)

type simpleEntry struct {
	k string
	v int
}
type simpleSnapshot = map[string]int

func newSimpleSnapshot() simpleSnapshot {
	return make(simpleSnapshot)
}

func squasher(snapshot *simpleSnapshot, entry simpleEntry) {
	(*snapshot)[entry.k] = entry.v
}

var simpleLog = NewLogSpec(
	newSimpleSnapshot,
	func(s *simpleSnapshot) simpleSnapshot { return maps.Clone(*s) },
	squasher,
)

func assertEq[V comparable](t *testing.T, val V, expected V) {
	t.Helper()
	if val != expected {
		t.Errorf("expected %#v to equal %#v but they did not.", val, expected)
	}
}

func TestLog(t *testing.T) {
	log := simpleLog.NewEmptyLog()

	log.Append(simpleEntry{"a", 1})
	assertEq(t, (*log.Last())["a"], 1)
	assertEq(t, log.LenLogical(), 1)

	log.Append(simpleEntry{"a", 2})
	assertEq(t, (*log.Last())["a"], 2)
	assertEq(t, log.LenLogical(), 2)

	log.Append(simpleEntry{"b", 3})
	assertEq(t, (*log.Last())["a"], 2)
	assertEq(t, (*log.Last())["b"], 3)
	assertEq(t, log.LenLogical(), 3)

	log.SquashFirstN(1)
	assertEq(t, (*log.Last())["a"], 2)
	assertEq(t, log.LenLogical(), 3)
	assertEq(t, log.LenActual(), 2)
}
