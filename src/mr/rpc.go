package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// GetFileNameReply 这个结构体是用于worker向coordinator请求文件名的回复
type GetFileNameForMapReply struct {
	FileName string
}

// MapInfoArgs 当请求不带参数时使用这个默认的args实现
type MapInfoArgs struct {
	Number int
}

type MapInfoReply struct {
	TaskNums         int
	TaskFinishedNums int
	NReduce          int
	Number           int
}

type MapFinishedArgs struct {
	WorkNumber            int
	IntermediateFilesName [][]string
	FileName              string
}

type MapFinishedReply struct {
	SUCCESS bool
}

type ReduceInfoArgs struct {
	Number int
}

type ReduceInfoReply struct {
	TaskNums         int
	TaskFinishedNums int
	NReduce          int
	Number           int
}

type GetFileNameForReduceReply struct {
	FileNames []string
}

type ReduceFinishedArgs struct {
	WorkNumber int
	FileNames  []string
}

type ReduceFinishedReply struct {
	SUCCESS bool
}

type MapCrashArgs struct {
	WorkNumber int
	FileName   string
}

type MapCrashReply struct {
	SUCCESS bool
}

type ReduceCrashArgs struct {
	WorkNumber int
}

type ReduceCrashReply struct {
	SUCCESS bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
