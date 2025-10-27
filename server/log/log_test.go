package log

import (
	"maps"
	"testing"
)

type simpleEntry struct {
	k string
	v int
}
type simpleSnapshot map[string]int

func (snapshot simpleSnapshot) ApplyEntry(entry simpleEntry) {
	snapshot[entry.k] = entry.v
}

func (snapshot simpleSnapshot) Clone() simpleSnapshot {
	return maps.Clone(snapshot)
}

func assertEq[V comparable](t *testing.T, val V, expected V) {
	t.Helper()
	if val != expected {
		t.Errorf("expected %#v to equal %#v but they did not.", val, expected)
	}
}

func TestLog(t *testing.T) {
	log := NewLog(make(simpleSnapshot), 0)

	log.Append(simpleEntry{"a", 1})
	assertEq(t, (*log.Latest())["a"], 1)
	assertEq(t, log.LenLogical(), 1)

	log.Append(simpleEntry{"a", 2})
	assertEq(t, (*log.Latest())["a"], 2)
	assertEq(t, log.LenLogical(), 2)

	log.Append(simpleEntry{"b", 3})
	assertEq(t, (*log.Latest())["a"], 2)
	assertEq(t, (*log.Latest())["b"], 3)
	assertEq(t, log.LenLogical(), 3)

	log.SquashFirstN(1)
	assertEq(t, (*log.Latest())["a"], 2)
	assertEq(t, log.LenLogical(), 3)
	assertEq(t, log.LenActual(), 2)
}
