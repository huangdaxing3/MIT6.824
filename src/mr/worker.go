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

//// 对临时文件进行重命名
//func yReduceFile(tmpFile string, taskN int) {
//	finalFile := fmt.Sprintf("mr-out-%d", taskN)
//	os.Rename(tmpFile, finalFile)
//}

//func gettmpFile(mapTaskN int, redTaskN int) string {
//	//fmt.Printf("%d %d\n", mapTaskN, redTaskN)
//	return fmt.Sprintf("mr-%d-%d", mapTaskN, redTaskN)
//}

//func ytmpFile(tmpFile string, mapTaskN int, redTaskN int) {
//	finalFile := gettmpFile(mapTaskN, redTaskN)
//	os.Rename(tmpFile, finalFile)
//}

func doMap(filename string, taskNum int, nReduceTasks int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Line 53: cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("line 57: cannot read %v", filename)
	}
	defer file.Close()

	kva := mapf(filename, string(content))

	tmpFiles := []*os.File{}
	tmp := []string{}
	encoders := []*json.Encoder{}
	for r := 0; r < nReduceTasks; r++ {
		ifile, err := ioutil.TempFile(".", "")
		if err != nil {
			log.Fatalf("cannot open tmpfile")
		}
		tmpFiles = append(tmpFiles, ifile)
		iname := ifile.Name()
		tmp = append(tmp, iname)
		enc := json.NewEncoder(ifile)
		encoders = append(encoders, enc)
	}

	for _, kv := range kva {
		encoders[ihash(kv.Key)%nReduceTasks].Encode(&kv)
	}
	for _, f := range tmpFiles {
		f.Close()
	}
	for r := 0; r < nReduceTasks; r++ {
		finalFile := fmt.Sprintf("mr-%d-%d", taskNum, r)
		os.Rename(tmp[r], finalFile)
	}
}

func doReduce(taskNum int, nMapTasks int, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for m := 0; m < nMapTasks; m++ {
		iFilename := fmt.Sprintf("mr-%d-%d", m, taskNum)
		file, err := os.Open(iFilename)
		if err != nil {
			log.Fatalf("line : 96 cannot open %v", iFilename)
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
	// 排序
	sort.Sort(ByKey(kva))
	ofile, err := ioutil.TempFile(".", "")
	if err != nil {
		log.Fatalf("cannot open tmpfile")
	}
	oname := ofile.Name()

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
	finalFile := fmt.Sprintf("mr-out-%d", taskNum)
	os.Rename(oname, finalFile)
	//yReduceFile(oname, taskNum)
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
		switch reply.TaskType {
		case Map:
			doMap(reply.MapFile, reply.TaskNum, reply.NReduceTasks, mapf)
		case Reduce:
			doReduce(reply.TaskNum, reply.NMapTasks, reducef)
		case Done:
			fmt.Println("tasks done")
			os.Exit(0)
		default:
			fmt.Errorf("bad task type %s", reply.TaskType)
		}
		//args.TaskInfo = reply.TaskInfo
		fargs := TaskArgs{
			TaskType: reply.TaskType,
			TaskNum:  reply.TaskNum}
		freply := TaskReply{}
		call("Coordinator.TaskDone", &fargs, &freply)
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
