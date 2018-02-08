package turtleDB

const (
	// VerbosityError represents error logs
	VerbosityError Verbosity = 1 << iota
	// VerbositySuccess represents success logs
	VerbositySuccess
	// VerbosityNotification represents notification logs
	VerbosityNotification
)

const (
	// DefaultVerbosity is the default verbosity level
	DefaultVerbosity = VerbosityError
	// AllVerbosity includes all verbosity levels
	AllVerbosity = VerbosityError | VerbositySuccess | VerbosityNotification
)

// Verbosity represents verbosity level
type Verbosity uint8

// CanError will return if a verbosity level supports error logs
func (v Verbosity) CanError() (can bool) {
	return v&VerbosityError != 0
}

// CanSuccess will return if a verbosity level supports success logs
func (v Verbosity) CanSuccess() (can bool) {
	return v&VerbositySuccess != 0
}

// CanNotify will return if a verbosity level supports notification logs
func (v Verbosity) CanNotify() (can bool) {
	return v&VerbosityNotification != 0
}
