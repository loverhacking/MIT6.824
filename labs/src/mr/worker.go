package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use for key sorting in reduce phase
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := new(AskTaskArgs)
		reply := new(AskTaskReply)

		// if worker can not connect master, return immediately
		if !call("Master.AskTask", args, reply) {
			return
		}

		switch reply.TaskType {
		case MapTask:
			doMapTask(mapf, reply)
		case ReduceTask:
			doReduceTask(reducef, reply)
		case WaitTask:
			time.Sleep(time.Second) // sleep for a while and ask again
		case ExitTask:
			return
		}
	}

}

func doMapTask(mapf func(string, string) []KeyValue, reply *AskTaskReply) {
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	file.Close()

	intermediate := []KeyValue{}
	kva := mapf(reply.FileName, string(content))
	intermediate = append(intermediate, kva...)

	buckets := make([][]KeyValue, reply.NReduce)
	for _, kv := range intermediate {
		reduceTaskID := ihash(kv.Key) % reply.NReduce
		buckets[reduceTaskID] = append(buckets[reduceTaskID], kv)
	}

	// save intermediate result
	for reduceTaskID, bucket := range buckets {
		outFileName := fmt.Sprintf("mr-%d-%d", reply.TaskID, reduceTaskID)
		// create temp file in the current directory
		outFile, err := ioutil.TempFile(".", "mr-map-*")
		if err != nil {
			log.Fatalf("cannot create temporary file for %v", "mr-map-*")
		}

		enc := json.NewEncoder(outFile)
		for _, kv := range bucket {
			err := enc.Encode(&kv)
			if err != nil {
				outFile.Close()
				log.Fatalf("cannot write %v", "mr-map-*")
			}
		}

		err = outFile.Close()
		if err != nil {
			log.Fatalf("cannot close %v", "mr-map-*")
		}
		// ensure that nobody observes partially written files in the presence of crashes,
		// use the trick of using a temporary file and atomically renaming it once it is completely written.
		err = os.Rename(outFile.Name(), outFileName)
		if err != nil {
			log.Fatalf("cannot rename temporary file to %v", outFileName)
		}
	}

	reportArgs := ReportTaskArgs{
		TaskID:   reply.TaskID,
		TaskType: reply.TaskType,
	}
	reportReply := ReportTaskReply{}
	call("Master.ReportTask", &reportArgs, &reportReply)
}

func doReduceTask(reducef func(string, []string) string, reply *AskTaskReply) {
	reduceID := reply.TaskID
	kva := []KeyValue{}

	for mapID := 0; mapID < reply.NMap; mapID++ {
		fileName := fmt.Sprintf("mr-%d-%d", mapID, reduceID)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	oname := "mr-out-" + strconv.Itoa(reduceID)
	// create temp file in the current directory to avoid cross-device rename error
	ofile, err := ioutil.TempFile(".", "mr-out-*")
	if err != nil {
		log.Fatalf("cannot create temporary file for %v", "mr-out-*")
	}

	i := 0
	for i < len(kva) {
		j := i + 1

		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	err = ofile.Close()
	if err != nil {
		log.Fatalf("cannot close %v", "mr-out-*")
	}

	err = os.Rename(ofile.Name(), oname)
	if err != nil {
		log.Fatalf("cannot rename temporary file to %v", oname)
	}

	reportArgs := ReportTaskArgs{
		TaskID:   reply.TaskID,
		TaskType: reply.TaskType,
	}
	reportReply := ReportTaskReply{}
	call("Master.ReportTask", &reportArgs, &reportReply)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
