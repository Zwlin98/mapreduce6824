package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

type EmptyArgsOrReply struct {
}

type InterruptArgs struct {
	Source string
	Event  string
	Args   string
}

type RegisterArgs struct {
	Id string
}
type RegisterReply struct {
	NReduce int
}

type TaskArgs struct {
	Id     int
	Type   int
	Key    string
	MapTaskNum int
	Worker string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
