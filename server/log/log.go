package log

import "slices"

type LogSquasher[E any, S any] func(snapshot *S, entry E)

type LogSpec[E any, S any] struct {
	squasher       LogSquasher[E, S]
	snapshotCtor   func() S
	snapshotCloner func(*S) S
}

type Log[E any, S any] struct {
	spec           *LogSpec[E, S]
	headSnapshot   S
	tailSnapshot   S
	realFirstIndex uint64
	entries        []E
}

func NewLogSpec[E any, S any](
	snapshotCtor func() S,
	snapshotCloner func(*S) S,
	squasher LogSquasher[E, S],
) LogSpec[E, S] {
	return LogSpec[E, S]{
		squasher:       squasher,
		snapshotCtor:   snapshotCtor,
		snapshotCloner: snapshotCloner,
	}
}

func (spec *LogSpec[E, S]) NewEmptyLog() Log[E, S] {
	return Log[E, S]{
		spec:           spec,
		headSnapshot:   spec.snapshotCtor(),
		tailSnapshot:   spec.snapshotCtor(),
		realFirstIndex: 0,
		entries:        []E{},
	}
}

func (log *Log[E, S]) SquashFirstN(n uint64) {
	for i := range n {
		log.spec.squasher(&log.headSnapshot, log.entries[i])
	}
	log.entries = log.entries[n:]
	log.realFirstIndex += n
}

type LogEntryPredicate[Entry any] func(entry Entry) bool

func (log *Log[E, S]) SquashUntil(predicate LogEntryPredicate[E]) {
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

func (log *Log[E, S]) Append(entry E) {
	log.spec.squasher(&log.tailSnapshot, entry)
	log.entries = append(log.entries, entry)
}

func (log *Log[E, S]) Last() *S {
	return &log.tailSnapshot
}

func (log *Log[E, S]) LenLogical() int {
	return int(log.realFirstIndex) + len(log.entries)
}

func (log *Log[E, S]) LenActual() int {
	return len(log.entries)
}
