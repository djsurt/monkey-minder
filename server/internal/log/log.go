package log

import (
	"errors"
	"slices"
)

type Snapshot[Entry any, Self any] interface {
	ApplyEntry(Entry) error
	Clone() Self
}

type Log[E any, S Snapshot[E, S]] struct {
	headSnapshot   S
	tailSnapshot   S
	realFirstIndex nonzeroIndex
	entries        []E
}

func NewLog[E any, S Snapshot[E, S]](
	initialSnapshot S,
	indexOffset uint64,
) Log[E, S] {
	return Log[E, S]{
		headSnapshot:   initialSnapshot.Clone(),
		tailSnapshot:   initialSnapshot,
		realFirstIndex: Index(1).promiseNonzero().offsetBy(indexOffset),
		entries:        []E{},
	}
}

// squash the log's first n items into its snapshot
func (log *Log[E, S]) SquashFirstN(n uint64) (err error) {
	for i := range n {
		err = log.headSnapshot.ApplyEntry(log.entries[i])
		if err != nil {
			return
		}
	}
	log.entries = log.entries[n:]
	log.realFirstIndex = log.realFirstIndex.offsetBy(n)
	return
}

// squash entries at the beginning of the log,
// up to (but not including) the first entry where the provided predicate returns true.
func (log *Log[Entry, _]) SquashUntil(predicate func(Entry) bool) (err error) {
	firstNonSquashed := slices.IndexFunc(log.entries, predicate)
	// none found
	if firstNonSquashed == -1 {
		return
	}
	// none to squash (b/c first element not squash candidate)
	if firstNonSquashed < 1 {
		return
	}

	err = log.SquashFirstN(uint64(firstNonSquashed - 1))
	return
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
	return int(log.realFirstIndex) - 1 + len(log.entries)
}

// get the length of the actually-stored portion of the log
// (in other words, not accounting for snapshots)
func (log *Log[E, S]) LenActual() int {
	return len(log.entries)
}

// get the LOGICAL index right before the log's first ACTUAL element.
// in other words, this will return what can be thought of as the index of our head snapshot.
func (log *Log[E, S]) IndexBeforeFirst() Index {
	return log.realFirstIndex.prior()
}

func (log *Log[E, S]) indexAfterLast() nonzeroIndex {
	return log.realFirstIndex.offsetBy(uint64(len(log.entries)))
}

// get the index right after the log's last element
func (log *Log[E, S]) IndexAfterLast() Index {
	return log.indexAfterLast().unwrap()
}

// get the index of the log's last element.
// this will not necissarily be the index of an actual element, as the log may be empty.
func (log *Log[E, S]) IndexOfLast() Index {
	return log.indexAfterLast().prior()
}

func (log *Log[E, S]) GetEntryAt(index Index) (entry *E, err error) {
	if index < log.realFirstIndex.unwrap() {
		return nil, errors.New("provided index lies below first actual entry of log")
	} else if index > log.IndexOfLast() {
		return nil, errors.New("provided index lies after last entry of log")
	} else {
		return &log.entries[index-log.realFirstIndex.unwrap()], nil
	}
}

// gets the latest entry in the log (if any) along with the index of said entry.
//
// WARNING: the entry pointer will not necissarily be valid after any subsequent
// squashes of the log, and should only really be used immediately after calling
func (log *Log[E, S]) GetEntryLatest() (entryMaybe *E, idx Index) {
	la := log.LenActual()
	if la >= 1 {
		return &log.entries[la-1], log.realFirstIndex.offsetBy(uint64(la - 1)).unwrap()
	} else {
		return nil, log.realFirstIndex.prior()
	}
}
