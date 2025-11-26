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

func (snapshot simpleSnapshot) ApplyEntry(entry simpleEntry) error {
	snapshot[entry.k] = entry.v
	return nil
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

func errorIfError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Error(err)
	}
}

func TestLog(t *testing.T) {
	log := NewLog(make(simpleSnapshot), 0)
	assertEq(t, log.IndexBeforeFirst(), Index(0))
	assertEq(t, log.IndexAfterLast(), Index(1))

	check, err := log.NewCheckpointAt(log.IndexOfLast())
	errorIfError(t, err)

	log.Append(simpleEntry{"a", 1})
	assertEq(t, log.LenLogical(), 1)
	assertEq(t, log.IndexBeforeFirst(), Index(0))
	assertEq(t, log.IndexAfterLast(), Index(2))

	assertEq(t, check.Index(), Index(0))
	errorIfError(t, check.AdvanceBy(1))
	assertEq(t, check.Index(), Index(1))
	assertEq(t, (*check.Data())["a"], 1)

	log.Append(simpleEntry{"a", 2})
	assertEq(t, log.LenLogical(), 2)
	assertEq(t, log.IndexBeforeFirst(), Index(0))
	assertEq(t, log.IndexAfterLast(), Index(3))

	log.Append(simpleEntry{"b", 3})
	assertEq(t, log.LenLogical(), 3)
	assertEq(t, log.IndexBeforeFirst(), Index(0))
	assertEq(t, log.IndexAfterLast(), Index(4))

	log.SquashFirstN(1)
	assertEq(t, log.LenLogical(), 3)
	assertEq(t, log.LenActual(), 2)
	assertEq(t, log.IndexBeforeFirst(), Index(1))
	assertEq(t, log.IndexAfterLast(), Index(4))
}

func TestCommit(t *testing.T) {
	log := NewLog(make(simpleSnapshot), 0)
	assertEq(t, log.IndexBeforeFirst(), Index(0))
	assertEq(t, log.IndexAfterLast(), Index(1))
	assertEq(t, log.GetCommitIndex(), Index(0))

	log.Append(simpleEntry{"a", 1})
	assertEq(t, (*log.Latest())["a"], 0)
	assertEq(t, log.IndexOfLast(), Index(1))
	assertEq(t, log.GetCommitIndex(), Index(0))

	log.Commit(log.IndexOfLast())
	assertEq(t, log.GetCommitIndex(), Index(1))
	assertEq(t, (*log.Latest())["a"], 1)

	log.Append(simpleEntry{"b", 2})
	log.Append(simpleEntry{"c", 3})
	log.Append(simpleEntry{"d", 4})

	assertEq(t, log.IndexOfLast(), Index(4))
	assertEq(t, log.GetCommitIndex(), Index(1))

	log.Commit(Index(3))
	assertEq(t, log.IndexOfLast(), Index(4))
	assertEq(t, log.GetCommitIndex(), Index(3))
	assertEq(t, (*log.Latest())["c"], 3)

	log.Commit(Index(4))
	assertEq(t, log.GetCommitIndex(), log.IndexOfLast())
	// Commits should be idempotent
	log.Commit(Index(4))
	assertEq(t, log.GetCommitIndex(), log.IndexOfLast())
	assertEq(t, (*log.Latest())["d"], 4)
}
