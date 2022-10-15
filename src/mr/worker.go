package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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
	fmt.Printf("map worker get task %d-%s\n", task.taskId, task.filename)

	tmp := make([][]KeyValue, task.n_reduce)
	for i := 0; i < task.n_reduce; i++ {
		tmp[i] = make([]KeyValue, 0)
	}

	file, err := os.Open(task.filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.filename)
	}
	file.Close()

	kva := mapf(task.filename, string(content))
	for _, kv := range kva {
		i := ihash(kv.Key)
		tmp[i%task.n_reduce] = append(tmp[i%task.n_reduce], kv)
	}

	for j := 0; j < task.n_reduce; j++ {
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
	for i := 0; i < task.n_map; i++ {
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
		call("coordinator.assigntask", &args, &reply)
		switch reply.task_Info.taskType {
		case Map:
			doMap(mapf, &reply.task_Info)
		case Reduce:
			doReduce(reducef, &reply.task_Info)
		case Wait:
			fmt.Println("wait tasks")
			time.Sleep(time.Second)
			continue
		case Done:
			fmt.Println("tasks done")
			return
		}
		args.task_Info = reply.task_Info
		call("coordinator.done", &args, &reply)
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
