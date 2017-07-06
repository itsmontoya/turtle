package types

//go:generate genny -pkg turtleDB -in=$HOME/go/src/github.com/Path94/turtle/bucket.go -out=bytes/bucket.go gen "Value=[]byte"
//go:generate genny -pkg turtleDB -in=$HOME/go/src/github.com/Path94/turtle/buckets.go -out=bytes/buckets.go gen "Value=[]byte"
//go:generate genny -pkg turtleDB -in=$HOME/go/src/github.com/Path94/turtle/rtxn.go -out=bytes/rtxn.go gen "Value=[]byte"
//go:generate genny -pkg turtleDB -in=$HOME/go/src/github.com/Path94/turtle/turtle.go -out=bytes/turtle.go gen "Value=[]byte"
//go:generate genny -pkg turtleDB -in=$HOME/go/src/github.com/Path94/turtle/txnBucket.go -out=bytes/txnBucket.go gen "Value=[]byte"
//go:generate genny -pkg turtleDB -in=$HOME/go/src/github.com/Path94/turtle/txnBuckets.go -out=bytes/txnBuckets.go gen "Value=[]byte"
//go:generate genny -pkg turtleDB -in=$HOME/go/src/github.com/Path94/turtle/utilities.go -out=bytes/utilities.go gen "Value=[]byte"
//go:generate genny -pkg turtleDB -in=$HOME/go/src/github.com/Path94/turtle/wtxn.go -out=bytes/wtxn.go gen "Value=[]byte"
