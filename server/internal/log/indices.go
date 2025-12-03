package log

type Index uint64

// an `Index` which is guaranteed to be non-zero, or in other words, is guaranteed
// to point to an actual log entry, rather than the spot before the first entry.
type nonzeroIndex uint64

// assert an index to be non-zero. if it is actually zero, panics.
func (idx Index) promiseNonzero() nonzeroIndex {
	if idx == Index(0) {
		panic("got index 0 when nonzero index expected")
	}
	return nonzeroIndex(idx)
}

// add a non-negative value to a known nonzero index, thus preserving guarantees.
func (nz nonzeroIndex) offsetBy(offset uint64) nonzeroIndex {
	return nonzeroIndex(uint64(nz) + offset)
}

// get the (not necissarily non-zero) index immediately preceding this index. as
// we are guaranteed to be at least one, this can be guaranteed to never overflow.
func (nz nonzeroIndex) prior() Index {
	return nz.unwrap() - 1
}

func (nz nonzeroIndex) unwrap() Index {
	return Index(nz)
}
