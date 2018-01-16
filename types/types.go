package types

//go:generate go install github.com/OneOfOne/genx/cmd/genx
//go:generate genx -pkg github.com/PathDNA/turtleDB -t "Value=[]byte" -o ./bytes/turtle.go
//go:generate go test ./...
