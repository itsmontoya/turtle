package turtleDB

// rTxn is a read transaction
type rTxn struct {
	// Original buckets
	*buckets
}

func (r *rTxn) clear() {
	r.buckets = nil
}
