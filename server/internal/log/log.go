package log

import (
	"errors"
	"fmt"
	"slices"
)

type Snapshot[Entry any, Self any] interface {
	ApplyEntry(Entry) error
	Clone() Self
}

type checkpointId uint64

type checkpointData[E any, S Snapshot[E, S]] struct {
	index    Index
	snapshot S
	// lowest index which we have not yet applied to this checkpoint's snapshot
	firstUnapplied Index
}

type Checkpoint[E any, S Snapshot[E, S]] struct {
	log *Log[E, S]
	id  checkpointId
}

type Log[E any, S Snapshot[E, S]] struct {
	headSnapshot   S
	tailSnapshot   S
	realFirstIndex nonzeroIndex
	entries        []E
	nextCheckpoint checkpointId
	checkpoints    map[checkpointId]*checkpointData[E, S]
}

func NewLog[E any, S Snapshot[E, S]](
	initialSnapshot S,
	indexOffset uint64,
) *Log[E, S] {
	return &Log[E, S]{
		headSnapshot:   initialSnapshot.Clone(),
		tailSnapshot:   initialSnapshot,
		realFirstIndex: Index(1).promiseNonzero().offsetBy(indexOffset),
		entries:        []E{},
		nextCheckpoint: checkpointId(0),
		checkpoints:    make(map[checkpointId]*checkpointData[E, S]),
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
func (log *Log[E, S]) Append(entry E) (err error) {
	err = log.tailSnapshot.ApplyEntry(entry)
	if err != nil {
		return
	}
	log.entries = append(log.entries, entry)
	err = log.updateCheckpoints()
	if err != nil {
		return
	}
	return
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

func (log *Log[E, S]) getEntryAtUnchecked(index Index) (entry *E) {
	return &log.entries[index-log.realFirstIndex.unwrap()]
}

func (log *Log[E, S]) GetEntryAt(index Index) (entry *E, err error) {
	if index < log.realFirstIndex.unwrap() {
		return nil, errors.New("provided index lies below first actual entry of log")
	} else if index > log.IndexOfLast() {
		return nil, errors.New("provided index lies after last entry of log")
	} else {
		return log.getEntryAtUnchecked(index), nil
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

// returns true iif the provided index is the index of a current entry in the log
func (log *Log[E, S]) HasEntryAt(index Index) bool {
	return log.IndexBeforeFirst() < index && index < log.IndexAfterLast()
}

func (ckpt *checkpointData[E, S]) updateSnapshot(log *Log[E, S]) error {
	for ckpt.firstUnapplied <= ckpt.index && log.HasEntryAt(ckpt.firstUnapplied) {
		applyErr := ckpt.snapshot.ApplyEntry(*log.getEntryAtUnchecked(ckpt.firstUnapplied))
		if applyErr != nil {
			return fmt.Errorf("error encountered while applying log entry at index %v: %w", ckpt.firstUnapplied, applyErr)
		}
		ckpt.firstUnapplied++
	}
	return nil
}

func (log *Log[E, S]) updateCheckpoints() error {
	for id, ckpt := range log.checkpoints {
		err := ckpt.updateSnapshot(log)
		return fmt.Errorf("error while updating checkpoint #%v: %w", id, err)
	}
	return nil
}

func (log *Log[E, S]) NewCheckpointAt(index Index) (*Checkpoint[E, S], error) {
	if !(log.HasEntryAt(index) || index == log.IndexBeforeFirst()) {
		return nil, errors.New("provided index must be the index of a valid element, or the index immediately before the first element, or the index immediately after the last element")
	}

	ckpt := checkpointData[E, S]{
		index:          index,
		snapshot:       log.headSnapshot.Clone(),
		firstUnapplied: log.IndexBeforeFirst() + 1,
	}
	err := ckpt.updateSnapshot(log)
	if err != nil {
		return nil, fmt.Errorf("error encountered during initial fast-forward of a new log checkpoint with target index %v: %w", index, err)
	}

	id := log.nextCheckpoint
	log.nextCheckpoint++
	log.checkpoints[id] = &ckpt

	check := Checkpoint[E, S]{
		log: log,
		id:  id,
	}
	return &check, nil
}

func (check Checkpoint[E, S]) data() *checkpointData[E, S] {
	ckpt, ok := check.log.checkpoints[check.id]
	if !ok {
		panic("Checkpoint handle #%v has no corresponding data")
	}
	return ckpt
}

// the index this checkpoint is logically pointing at.
//
// this is usually also the index of a corresponding entry, unless the checkpoint
// is currently pointing at the space immediately before the start of the log or
// immediately after the end of the log.
func (check Checkpoint[E, S]) Index() Index {
	return check.data().index
}

// get the snapshot at the checkpoint
func (check Checkpoint[E, S]) Data() *S {
	return &check.data().snapshot
}

func (check Checkpoint[E, S]) AdvanceBy(amount uint64) error {
	ckpt := check.data()
	return check.AdvanceTo(ckpt.index + Index(amount))
}

func (check Checkpoint[E, S]) AdvanceTo(index Index) error {
	ckpt := check.data()
	if !(check.log.HasEntryAt(index) || index == check.log.IndexBeforeFirst()) {
		return fmt.Errorf("cannot advance checkpoint by requested amount, that would send us past the end of the log!")
	}
	if index < ckpt.index {
		return fmt.Errorf("cannot advance checkpoint by negative amount! requested advance to index %v but we were already at index %v.", index, ckpt.index)
	}
	ckpt.index = index
	return ckpt.updateSnapshot(check.log)
}

func (check Checkpoint[E, S]) Close() {
	delete(check.log.checkpoints, check.id)
}

// WARNING: returned snapshot must be cloned before it is re-used elsewhere
func (log *Log[E, S]) biggestSnapshotAtOrBelow(index Index) (S, Index) {
	// FIXME implement non-trivial cases
	return log.headSnapshot, log.IndexBeforeFirst()
}

// Truncate everything from [idx, ...)
func (log *Log[E, S]) TruncateAt(index Index) error {
	if index <= log.IndexBeforeFirst() {
		return errors.New("cannot truncate to index which lies before the start of the log")
	}
	for _, ckpt := range log.checkpoints {
		if ckpt.index >= index {
			return errors.New("cannot truncate off entries at or beyond any still-live checkpoints")
		}
	}

	// trivial case, don't need to truncate
	if index >= log.IndexAfterLast() {
		return nil
	}

	newSnapshot, baseIndex := log.biggestSnapshotAtOrBelow(index)
	// clone the snapshot so we can modify it
	newSnapshot = newSnapshot.Clone()
	// apply any subsequent changes after that snapshot
	for i := baseIndex + 1; i < index; i++ {
		applyErr := newSnapshot.ApplyEntry(*log.getEntryAtUnchecked(i))
		// can safely assume these won't fail to apply since we must have already successfully applied them before
		if applyErr != nil {
			panic(applyErr)
		}
	}

	// truncate the actual internal list
	log.entries = log.entries[:index-log.realFirstIndex.unwrap()]
	// write the corrected snapshot
	log.tailSnapshot = newSnapshot

	return nil
}
