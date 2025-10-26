package log

import "slices"

type LogSquasher[Entry any, Snapshot any] func(prevSnapshot *Snapshot, entries []Entry) Snapshot

type LogQuerier[E any, S any, Q any, V any] func(snapshot *S, entries []E, query Q) V

type Log[E any, S any, Q any, V any] struct {
	squasher       LogSquasher[E, S]
	querier        LogQuerier[E, S, Q, V]
	snapshot       *S
	realFirstIndex uint64
	entries        []E
}

func NewEmptyLog[E any, S any, Q any, V any](squasher LogSquasher[E, S], querier LogQuerier[E, S, Q, V]) Log[E, S, Q, V] {
	return Log[E, S, Q, V]{
		squasher:       squasher,
		querier:        querier,
		snapshot:       nil,
		realFirstIndex: 0,
		entries:        []E{},
	}
}

func (log *Log[E, S, Q, V]) SquashFirstN(n uint64) {
	toSquash := log.entries[:n+1]
	newValues := log.entries[n+1:]

	newSnapshot := log.squasher(log.snapshot, toSquash)

	log.snapshot = &newSnapshot
	log.entries = newValues
	log.realFirstIndex += n
}

type LogEntryPredicate[Entry any] func(entry Entry) bool

func (log *Log[E, S, Q, V]) SquashUntil(predicate LogEntryPredicate[E]) {
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

func (log *Log[E, S, Q, V]) Query(query Q) V {
	return log.querier(log.snapshot, log.entries, query)
}

func (log *Log[E, S, Q, V]) Append(entry E) {
	log.entries = append(log.entries, entry)
}
