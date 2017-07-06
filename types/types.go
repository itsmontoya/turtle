package types

//go:generate genny -pkg bytes -in=$HOME/go/src/github.com/itsmontoya/turtle/bucket.go -out=bytes/bucket.go gen "Value=[]byte"
//go:generate genny -pkg bytes -in=$HOME/go/src/github.com/itsmontoya/turtle/buckets.go -out=bytes/buckets.go gen "Value=[]byte"
//go:generate genny -pkg bytes -in=$HOME/go/src/github.com/itsmontoya/turtle/rtxn.go -out=bytes/rtxn.go gen "Value=[]byte"
//go:generate genny -pkg bytes -in=$HOME/go/src/github.com/itsmontoya/turtle/turtle.go -out=bytes/turtle.go gen "Value=[]byte"
//go:generate genny -pkg bytes -in=$HOME/go/src/github.com/itsmontoya/turtle/txnBucket.go -out=bytes/txnBucket.go gen "Value=[]byte"
//go:generate genny -pkg bytes -in=$HOME/go/src/github.com/itsmontoya/turtle/txnBuckets.go -out=bytes/txnBuckets.go gen "Value=[]byte"
//go:generate genny -pkg bytes -in=$HOME/go/src/github.com/itsmontoya/turtle/utilities.go -out=bytes/utilities.go gen "Value=[]byte"
//go:generate genny -pkg bytes -in=$HOME/go/src/github.com/itsmontoya/turtle/wtxn.go -out=bytes/wtxn.go gen "Value=[]byte"
