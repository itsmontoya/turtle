package bytes

// New will return a new database
func New(name, path string, mfn MarshalFn, ufn UnmarshalFn) (dbp *DB, err error) {
	var db DB
	if db.turtle, err = newTurtle(name, path, mfn, ufn); err != nil {
		return
	}

	dbp = &db
	return
}

// DB is a database
type DB struct {
	*turtle
}
