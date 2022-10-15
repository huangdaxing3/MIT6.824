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

const (
	Map    = 0
	Reduce = 1
	Wait   = 2
	Done   = 3
)

type Coordinator struct {
	// Your definitions here.
	nmap          int
	nreduce       int
	haveReduce    bool
	lock          sync.Mutex
	maptasked     map[int]Task
	maptasking    map[int]Task
	reducetasked  map[int]Task
	reducetasking map[int]Task
}

// Your code here -- RPC handlers for the worker to call.
type Task struct {
	filename  string
	taskType  int
	taskId    int
	n_reduce  int
	n_map     int
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

func (c *Coordinator) getTask(args *RPCArgs, reply *RPCReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	currentTime := time.Now().Unix()
	for k, v := range c.maptasking {
		if currentTime-v.timeStamp > 10 {
			c.maptasked[k] = v
			delete(c.maptasking, k)
			fmt.Printf("check map task %d\n", k)
		}
	}
	for k, v := range c.reducetasking {
		if currentTime-v.timeStamp > 10 {
			c.reducetasked[k] = v
			delete(c.reducetasking, k)
			fmt.Printf("check reduce task %d\n", k)
		}
	}

	if len(c.maptasked) > 0 {
		for k, v := range c.maptasked {
			v.timeStamp = time.Now().Unix()
			reply.task_Info = v
			c.maptasking[k] = v
			delete(c.maptasked, k)
			fmt.Printf("distribute map task %d\n", reply.task_Info.taskId)
			return nil
		}
	} else if len(c.maptasking) > 0 {
		reply.task_Info = Task{taskType: Wait}
		return nil
	}

	if !c.haveReduce {
		for i := 0; i < c.nreduce; i++ {
			c.reducetasked[i] = Task{
				taskType:  Reduce,
				taskId:    i,
				n_reduce:  c.nreduce,
				n_map:     c.nmap,
				timeStamp: time.Now().Unix(),
			}
		}
		c.haveReduce = true
	}

	if len(c.reducetasked) > 0 {
		for k, v := range c.reducetasked {
			v.timeStamp = time.Now().Unix()
			reply.task_Info = v
			c.reducetasking[k] = v
			delete(c.reducetasked, k)
			fmt.Printf("distribute reduce task %d\n", k)
			return nil
		}
	} else if len(c.reducetasking) > 0 {
		reply.task_Info = Task{taskType: Wait}
	} else {
		reply.task_Info = Task{taskType: Done}
	}
	return nil
}

func (c *Coordinator) TaskDone(args *RPCArgs, reply *RPCReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	switch args.task_Info.taskType {
	case Map:
		delete(c.maptasking, args.task_Info.taskId)
		fmt.Printf("Map task %d done, %d tasks left\n", args.task_Info.taskId, len(c.maptasking)+len(c.maptasked))
	case Reduce:
		delete(c.reducetasking, args.task_Info.taskId)
		fmt.Printf("Reduce task %d done, %d tasks left\n", args.task_Info.taskId, len(c.reducetasking)+len(c.reducetasked))
	}
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if len(c.maptasked) == 0 && len(c.maptasking) == 0 && len(c.reducetasking) == 0 && len(c.reducetasked) == 0 {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.lock.Lock()
	defer c.lock.Unlock()

	c.maptasked = make(map[int]Task)
	c.maptasking = make(map[int]Task)
	c.reducetasked = make(map[int]Task)
	c.reducetasking = make(map[int]Task)

	num := len(files)
	for i, file := range files {
		c.maptasked[i] = Task{
			filename:  file,
			taskType:  Map,
			taskId:    i,
			n_reduce:  nReduce,
			n_map:     num,
			timeStamp: time.Now().Unix(),
		}
	}

	c.server()
	return &c
}
