package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type Coordinator struct {
	// Your definitions here.
	InputFileNames        []string
	IntermediateFilesName [][]string
	MapRunning            map[string]bool
	MapFinished           map[string]bool
	MapCrash              []int
	ReduceRunning         []bool
	ReduceFinished        []bool
	ReduceCrash           []int
	MapFinishedCount      int
	ReduceFinishedCount   int
	nReduce               int
	number                int
	mu                    sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// GetOneFileForMap 这一行代码是worker中运行map时向coordinator请求input的文件名
func (c *Coordinator) GetOneFileForMap(args *MapInfoArgs, reply *GetFileNameForMapReply) error {
	reply.FileName = "NO_AVAILABLE_FILE"
	c.mu.Lock()
	for _, name := range c.InputFileNames {
		if (c.MapRunning[name] == false) && (c.MapFinished[name] == false) {
			reply.FileName = name
			c.MapRunning[name] = true
			break
		}
	}
	c.mu.Unlock()
	return nil
}

// GetMapInfo 这一行代码时worker中运行map时的一些信息
func (c *Coordinator) GetMapInfo(args *MapInfoArgs, reply *MapInfoReply) error {
	c.mu.Lock()
	if args.Number == 0 {
		c.number += 1
		reply.Number = c.number
	} else {
		reply.Number = args.Number
	}
	reply.NReduce = c.nReduce
	reply.TaskNums = len(c.InputFileNames)
	reply.TaskFinishedNums = c.MapFinishedCount
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) HandleMapFinished(args *MapFinishedArgs, reply *MapFinishedReply) error {
	c.mu.Lock()
	for _, id := range c.MapCrash {
		if id == args.WorkNumber {
			c.mu.Unlock()
			reply.SUCCESS = false
			return nil
		}
	}
	for i := 0; i < c.nReduce; i++ {
		c.IntermediateFilesName[i] = append(c.IntermediateFilesName[i], args.IntermediateFilesName[i]...)
	}
	c.MapRunning[args.FileName] = false
	c.MapFinished[args.FileName] = true
	c.MapFinishedCount += 1
	c.mu.Unlock()
	reply.SUCCESS = true
	return nil
}

// GetMapInfo 这一行代码时worker中运行map时的一些信息
func (c *Coordinator) GetReduceInfo(args *ReduceInfoArgs, reply *ReduceInfoReply) error {
	c.mu.Lock()
	if args.Number == 0 {
		c.number += 1
		reply.Number = c.number
	} else {
		reply.Number = args.Number
	}
	reply.NReduce = c.nReduce
	reply.TaskNums = len(c.InputFileNames) * c.nReduce
	reply.TaskFinishedNums = c.ReduceFinishedCount
	c.mu.Unlock()
	return nil
}

// GetOneFileForMap 这一行代码是worker中运行map时向coordinator请求input的文件名
func (c *Coordinator) GetFilesForReduce(args *ReduceInfoArgs, reply *GetFileNameForReduceReply) error {
	reply.FileNames = []string{}
	c.mu.Lock()
	if (c.ReduceRunning[args.Number%c.nReduce] == false) && (c.ReduceFinished[args.Number%c.nReduce] == false) {
		reply.FileNames = append(reply.FileNames, c.IntermediateFilesName[args.Number%c.nReduce]...)
		c.ReduceRunning[args.Number%c.nReduce] = true
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) HandleReduceFinished(args *ReduceFinishedArgs, reply *ReduceFinishedReply) error {
	c.mu.Lock()
	for _, id := range c.ReduceCrash {
		if id == args.WorkNumber {
			c.mu.Unlock()
			reply.SUCCESS = false
			return nil
		}
	}
	c.ReduceRunning[args.WorkNumber%c.nReduce] = false
	c.ReduceFinished[args.WorkNumber%c.nReduce] = true
	c.ReduceFinishedCount += len(c.IntermediateFilesName[args.WorkNumber%c.nReduce])
	c.mu.Unlock()
	reply.SUCCESS = true
	return nil
}

func (c *Coordinator) DetectMapCrash(args *MapCrashArgs, reply *MapCrashReply) error {
	now := time.Now()
	t := time.Since(now)
	for t < time.Second*10 {
		c.mu.Lock()
		flag := c.MapFinished[args.FileName]
		c.mu.Unlock()
		if flag {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
		t = time.Since(now)
	}
	fmt.Printf("map worker %d crashed\n", args.WorkNumber)
	c.mu.Lock()
	c.MapRunning[args.FileName] = false
	c.MapFinished[args.FileName] = false
	c.MapCrash = append(c.MapCrash, args.WorkNumber)
	c.mu.Unlock()
	reply.SUCCESS = true
	return nil
}

func (c *Coordinator) DetectReduceCrash(args *ReduceCrashArgs, reply *ReduceCrashReply) error {
	now := time.Now()
	t := time.Since(now)
	for t < time.Second*10 {
		c.mu.Lock()
		flag := c.ReduceFinished[args.WorkNumber%c.nReduce]
		c.mu.Unlock()
		if flag {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
		t = time.Since(now)
	}
	fmt.Printf("reduce worker %d crashed\n", args.WorkNumber)
	c.mu.Lock()
	c.ReduceRunning[args.WorkNumber%c.nReduce] = false
	c.ReduceFinished[args.WorkNumber%c.nReduce] = false
	c.ReduceCrash = append(c.ReduceCrash, args.WorkNumber)
	c.mu.Unlock()
	reply.SUCCESS = true
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	c.mu.Lock()
	if c.ReduceFinishedCount == len(c.InputFileNames)*c.nReduce {
		ret = true
		for _, id := range c.ReduceCrash {
			os.Remove(fmt.Sprintf("mr-out-%d", id))
		}
	}
	c.mu.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// 初始化 coordinator
	c.mu.Lock()
	c.InputFileNames = files
	c.IntermediateFilesName = make([][]string, nReduce)
	c.MapRunning = make(map[string]bool)
	c.ReduceFinished = make([]bool, nReduce)
	c.MapFinished = make(map[string]bool)
	c.ReduceRunning = make([]bool, nReduce)
	c.MapCrash = []int{}
	c.ReduceCrash = []int{}
	c.MapFinishedCount = 0
	c.nReduce = nReduce
	c.ReduceFinishedCount = 0
	c.number = 0
	c.mu.Unlock()
	c.server()
	return &c
}
