package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// worker to master
type W2mRegisterArg struct {
	SockName string
}

type W2mRegisterReply struct {
	Success  bool
	WorkerID int
}

type W2mMapDoneArg struct {
	FilePaths []string
	TaskID    int
	WorkerID  int
}

type W2mMapDoneReply struct {
}

type W2mReduceDoneArg struct {
	TaskID   int
	WorkerID int
}

type W2mReduceDoneReply struct {
}

// Master to worker
type M2wMapArgs struct {
	FilePath string
	TaskID   int
	NReduce  int
}

type M2wMapReply struct {
}

type M2wReduceArgs struct {
	FilePaths []string
	TaskID    int
}

type M2wReduceReply struct {
}

type M2wShutdownArgs struct {
}

type M2wShutdownReply struct {
	WorkerID int
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
