package log

import "slices"

type Snapshot[Entry any, Self any] interface {
	ApplyEntry(Entry)
	Clone() Self
}

type Log[E any, S Snapshot[E, S]] struct {
	headSnapshot   S
	tailSnapshot   S
	realFirstIndex uint64
	entries        []E
}

func NewLog[E any, S Snapshot[E, S]](
	initialSnapshot S,
	indexOffset uint64,
) Log[E, S] {
	return Log[E, S]{
		headSnapshot:   initialSnapshot.Clone(),
		tailSnapshot:   initialSnapshot,
		realFirstIndex: indexOffset,
		entries:        []E{},
	}
}

// squash the log's first n items into its snapshot
func (log *Log[E, S]) SquashFirstN(n uint64) {
	for i := range n {
		log.headSnapshot.ApplyEntry(log.entries[i])
	}
	log.entries = log.entries[n:]
	log.realFirstIndex += n
}

// squash entries at the beginning of the log,
// up to (but not including) the first entry where the provided predicate returns true.
func (log *Log[Entry, _]) SquashUntil(predicate func(Entry) bool) {
	firstNonSquashed := slices.IndexFunc(log.entries, predicate)
	// none found
	if firstNonSquashed == -1 {
		return
	}
	// none to squash (b/c first element not squash candidate)
	if firstNonSquashed < 1 {
		return
	}

	log.SquashFirstN(uint64(firstNonSquashed - 1))
}

// append a new entry to the log
func (log *Log[E, S]) Append(entry E) {
	log.tailSnapshot.ApplyEntry(entry)
	log.entries = append(log.entries, entry)
}

// a snapshot at the current state of the log.
// this operation is trivial and will not have any significant performance impact.
func (log *Log[E, S]) Latest() *S {
	return &log.tailSnapshot
}

func (log *Log[E, S]) LenLogical() int {
	return int(log.realFirstIndex) + len(log.entries)
}

func (log *Log[E, S]) LenActual() int {
	return len(log.entries)
}
