package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

func doMap(mapf func(string, string) []KeyValue, task *Task) {
	fmt.Printf("map worker get task %d-%s\n", task.taskId, task.Filename)

	tmp := make([][]KeyValue, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		tmp[i] = make([]KeyValue, 0)
	}

	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()

	kva := mapf(task.Filename, string(content))
	for _, kv := range kva {
		tmp[ihash(kv.Key)%task.NReduce] = append(tmp[ihash(kv.Key)%task.NReduce], kv)
	}

	for j := 0; j < task.NReduce; j++ {
		if len(tmp[j]) == 0 {
			continue
		}
		oname := fmt.Sprintf("mr-%d-%d", task.taskId, j)
		ofile, _ := ioutil.TempFile("./", "tmp_")
		enc := json.NewEncoder(ofile)
		for _, kv := range tmp[j] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Json encode err: K:V %s:%s", kv.Key, kv.Value)
			}
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}
}

func doReduce(reducef func(string, []string) string, task *Task) {
	fmt.Printf("reduce worker get task %d\n", task.taskId)

	tmp := make([]KeyValue, 0)
	for i := 0; i < task.NMap; i++ {
		iname := fmt.Sprintf("mr-%d-%d", i, task.taskId)
		ifile, _ := os.Open(iname)
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			tmp = append(tmp, kv)
		}
		ifile.Close()
	}
	sort.Sort(ByKey(tmp))
	oname := fmt.Sprintf("mr-out-%d", task.taskId)
	ofile, _ := ioutil.TempFile("./", "tmp_")

	for i := 0; i < len(tmp); {
		j := i + 1
		for j < len(tmp) && tmp[j].Key == tmp[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, tmp[k].Value)
		}
		output := reducef(tmp[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", tmp[i].Key, output)
		i = j
	}
	ofile.Close()
	os.Rename(ofile.Name(), oname)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		args := RPCArgs{}
		reply := RPCReply{}
		call("Coordinator.GetTask", &args, &reply)
		switch reply.TaskInfo.taskType {
		case Map:
			doMap(mapf, &reply.TaskInfo)
		case Reduce:
			doReduce(reducef, &reply.TaskInfo)
		case Wait:
			fmt.Println("wait tasks")
			time.Sleep(time.Second)
			continue
		case Done:
			fmt.Println("tasks done")
			return
		}
		args.TaskInfo = reply.TaskInfo
		call("Coordinator.TaskDone", &args, &reply)
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
