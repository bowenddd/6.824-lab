package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	mapInfoArgs := MapInfoArgs{Number: 0}
	mapInfoReply := MapInfoReply{}
	fileNameReply := GetFileNameForMapReply{}
	call("Coordinator.GetMapInfo", &mapInfoArgs, &mapInfoReply)
	mapInfoArgs.Number = mapInfoReply.Number
	for mapInfoReply.TaskFinishedNums < mapInfoReply.TaskNums {
		// 如果没有执行完成map就去一直循环执行map
		call("Coordinator.GetOneFileForMap", &mapInfoArgs, &fileNameReply)
		if fileNameReply.FileName != "NO_AVAILABLE_FILE" {
			go func(filename string) {
				mapCrashArgs := MapCrashArgs{WorkNumber: mapInfoReply.Number, FileName: filename}
				mapCrashReply := MapCrashReply{}
				call("Coordinator.DetectMapCrash", &mapCrashArgs, &mapCrashReply)
			}(fileNameReply.FileName)
			fmt.Printf("%d start handle map\n", mapInfoReply.Number)
			MapHandler(fileNameReply.FileName, mapInfoReply.Number, mapInfoReply.NReduce, mapf)
		}
		mapInfoArgs = MapInfoArgs{Number: 0}
		mapInfoReply = MapInfoReply{}
		call("Coordinator.GetMapInfo", &mapInfoArgs, &mapInfoReply)
	}
	reduceInfoArgs := ReduceInfoArgs{Number: 0}
	reduceInfoReply := ReduceInfoReply{}
	filesNameReply := GetFileNameForReduceReply{}
	call("Coordinator.GetReduceInfo", &reduceInfoArgs, &reduceInfoReply)
	reduceInfoArgs.Number = reduceInfoReply.Number
	for reduceInfoReply.TaskFinishedNums < reduceInfoReply.TaskNums {
		call("Coordinator.GetFilesForReduce", &reduceInfoArgs, &filesNameReply)
		if len(filesNameReply.FileNames) > 0 {
			go func() {
				reduceCrashArgs := ReduceCrashArgs{WorkNumber: mapInfoReply.Number}
				reduceCrashReply := ReduceCrashReply{}
				call("Coordinator.DetectReduceCrash", &reduceCrashArgs, &reduceCrashReply)
			}()
			fmt.Printf("%d start handle reduce\n", reduceInfoReply.Number)
			ReduceHandler(filesNameReply.FileNames, reduceInfoReply.Number, reducef)
		}
		reduceInfoArgs = ReduceInfoArgs{Number: 0}
		reduceInfoReply = ReduceInfoReply{}
		call("Coordinator.GetReduceInfo", &reduceInfoArgs, &reduceInfoReply)
		reduceInfoArgs.Number = reduceInfoReply.Number
		filesNameReply = GetFileNameForReduceReply{}
	}

	//uncomment to send the Example RPC to the coordinator.
	//CallExample()
}

func MapHandler(filename string, workNumber int, nReduce int, mapf func(string, string) []KeyValue) bool {
	tempfiles := make([]*os.File, nReduce)
	var tempKvs [][]KeyValue
	var index int

	for i := 0; i < nReduce; i++ {
		tempfiles[i], _ = ioutil.TempFile("./", "temp-*.json")
	}
	tempKvs = make([][]KeyValue, nReduce)

	file, _ := os.Open(filename)
	content, _ := ioutil.ReadAll(file)
	defer file.Close()
	kva := mapf(filename, string(content))
	intermediateFiles := make([][]string, nReduce)
	var intermediateFileName string
	for _, kv := range kva {
		index = ihash(kv.Key) % nReduce
		tempKvs[index] = append(tempKvs[index], kv)
	}
	for i := 0; i < nReduce; i++ {
		tempfile := tempfiles[i]
		enc := json.NewEncoder(tempfile)
		enc.Encode(tempKvs[i])
		intermediateFileName = fmt.Sprintf("out-%d-%d.json", workNumber, i)
		os.Rename(tempfile.Name(), intermediateFileName)
		intermediateFiles[i] = append(intermediateFiles[i], intermediateFileName)
	}
	mapFinishedArgs := MapFinishedArgs{WorkNumber: workNumber, IntermediateFilesName: intermediateFiles, FileName: filename}
	mapFinishedReply := MapFinishedReply{}
	call("Coordinator.HandleMapFinished", &mapFinishedArgs, &mapFinishedReply)
	return mapFinishedReply.SUCCESS
}

func ReduceHandler(filenames []string, workNumber int, reducef func(string, []string) string) bool {
	intermediate := []KeyValue{}
	for _, filename := range filenames {
		file, _ := os.Open(filename)
		content, _ := ioutil.ReadAll(file)
		defer file.Close()
		kva := []KeyValue{}
		json.Unmarshal(content, &kva)
		intermediate = append(intermediate, kva...)
	}
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", workNumber)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	defer ofile.Close()
	reduceFinishedArgs := ReduceFinishedArgs{WorkNumber: workNumber, FileNames: filenames}
	reduceFinishedReply := ReduceFinishedReply{}
	call("Coordinator.HandleReduceFinished", &reduceFinishedArgs, &reduceFinishedReply)
	return reduceFinishedReply.SUCCESS
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
