package types

//go:generate genny -pkg turtleDB -in=$HOME/go/src/github.com/Path94/turtleDB/bucket.go -out=bytes/bucket.go gen "Value=[]byte"
//go:generate genny -pkg turtleDB -in=$HOME/go/src/github.com/Path94/turtleDB/buckets.go -out=bytes/buckets.go gen "Value=[]byte"
//go:generate genny -pkg turtleDB -in=$HOME/go/src/github.com/Path94/turtleDB/rtxn.go -out=bytes/rtxn.go gen "Value=[]byte"
//go:generate genny -pkg turtleDB -in=$HOME/go/src/github.com/Path94/turtleDB/turtle.go -out=bytes/turtle.go gen "Value=[]byte"
//go:generate genny -pkg turtleDB -in=$HOME/go/src/github.com/Path94/turtleDB/txnBucket.go -out=bytes/txnBucket.go gen "Value=[]byte"
//go:generate genny -pkg turtleDB -in=$HOME/go/src/github.com/Path94/turtleDB/txnBuckets.go -out=bytes/txnBuckets.go gen "Value=[]byte"
//go:generate genny -pkg turtleDB -in=$HOME/go/src/github.com/Path94/turtleDB/utilities.go -out=bytes/utilities.go gen "Value=[]byte"
//go:generate genny -pkg turtleDB -in=$HOME/go/src/github.com/Path94/turtleDB/wtxn.go -out=bytes/wtxn.go gen "Value=[]byte"
