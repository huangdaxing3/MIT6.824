package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nmap          int
	nReduce       int
	haveReduce    bool
	lock          sync.Mutex
	mapTasked     map[int]Task
	mapTasking    map[int]Task
	reduceTasked  map[int]Task
	reduceTasking map[int]Task
	isDone        bool
}

const (
	Map    = 0
	Reduce = 1
	Wait   = 2
	Done   = 3
)

type Task struct {
	Filename  string
	taskType  int
	taskId    int
	NReduce   int
	NMap      int
	timeStamp int64
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

func (c *Coordinator) GetTask(args *RPCArgs, reply *RPCReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	currentTime := time.Now().Unix()
	for k, v := range c.mapTasking {
		if currentTime-v.timeStamp > 10 {
			c.mapTasked[k] = v
			delete(c.mapTasking, k)
			fmt.Printf("check map task %d\n", k)
		}
	}
	for k, v := range c.reduceTasking {
		if currentTime-v.timeStamp > 10 {
			c.reduceTasked[k] = v
			delete(c.reduceTasking, k)
			fmt.Printf("check reduce task %d\n", k)
		}
	}

	if len(c.mapTasked) > 0 {
		for k, v := range c.mapTasked {
			v.timeStamp = time.Now().Unix()
			reply.TaskInfo = v
			c.mapTasking[k] = v
			delete(c.mapTasked, k)
			fmt.Printf("distribute map task %d\n", reply.TaskInfo.taskId)
			return nil
		}
	} else if len(c.mapTasking) > 0 {
		reply.TaskInfo = Task{taskType: Wait}
		return nil
	}

	if !c.haveReduce {
		for i := 0; i < c.nReduce; i++ {
			c.reduceTasked[i] = Task{
				taskType:  Reduce,
				taskId:    i,
				NReduce:   c.nReduce,
				NMap:      c.nmap,
				timeStamp: time.Now().Unix()}
		}
		c.haveReduce = true
	}

	if len(c.reduceTasked) > 0 {
		for k, v := range c.reduceTasked {
			v.timeStamp = time.Now().Unix()
			reply.TaskInfo = v
			c.reduceTasking[k] = v
			delete(c.reduceTasked, k)
			fmt.Printf("distribute reduce task %d\n", k)
			return nil
		}
	} else if len(c.reduceTasking) > 0 {
		reply.TaskInfo = Task{taskType: Wait}
	} else {
		reply.TaskInfo = Task{taskType: Done}
		c.isDone = true
	}
	return nil
}

func (c *Coordinator) TaskDone(args *RPCArgs, reply *RPCReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	switch args.TaskInfo.taskType {
	case Map:
		delete(c.mapTasking, args.TaskInfo.taskId)
		fmt.Printf("Map task %d done, %d tasks left\n", args.TaskInfo.taskId, len(c.mapTasking)+len(c.mapTasked))
	case Reduce:
		delete(c.reduceTasking, args.TaskInfo.taskId)
		fmt.Printf("Reduce task %d done, %d tasks left\n", args.TaskInfo.taskId, len(c.reduceTasking)+len(c.reduceTasked))
	}
	return nil
}

func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.isDone == true {
		ret = true
	}
	//if len(c.mapTasked) == 0 && len(c.mapTasking) == 0 && len(c.reduceTasking) == 0 && len(c.reduceTasked) == 0 {
	//	ret = true
	//}

	return ret
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.lock.Lock()
	defer c.lock.Unlock()

	c.mapTasked = make(map[int]Task)
	c.mapTasking = make(map[int]Task)
	c.reduceTasked = make(map[int]Task)
	c.reduceTasking = make(map[int]Task)

	num := len(files)
	fmt.Printf("---coor_MakeCoordinator start\n")
	for i, file := range files {
		fmt.Printf("i=%d\n", i)
		fmt.Printf("filename: %s\n", file)
		c.mapTasked[i] = Task{
			Filename:  file,
			taskType:  Map,
			taskId:    i,
			NReduce:   nReduce,
			NMap:      num,
			timeStamp: time.Now().Unix()}
	}
	c.haveReduce = false
	c.nReduce = nReduce
	c.nmap = num

	c.server()
	return &c
}
