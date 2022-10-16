package mr

import (
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
	lock          sync.Mutex
	nmapTasks     int
	nreduceTasks  int
	mapTasked     [8]bool
	reduceTasked  [10]bool
	mapFiles      [8]string
	mapTasking    [8]time.Time
	reduceTasking [10]time.Time
	isDone        bool
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

	reply.NReduceTasks = c.nreduceTasks
	reply.NMapTasks = c.nmapTasks

	for {
		mapDone := true
		for i, done := range c.mapTasked {
			if !done {
				if c.mapTasking[i].IsZero() || time.Since(c.mapTasking[i]).Seconds() > 10 {
					reply.TaskType = Map
					reply.TaskNum = i
					reply.MapFile = c.mapFiles[i]
					c.mapTasking[i] = time.Now()
					return nil
				} else {
					mapDone = false
				}
			}
		}
		if !mapDone {
		} else {
			break
		}
	}

	for {
		reduceDone := true
		for i, done := range c.reduceTasked {
			if !done {
				if c.reduceTasking[i].IsZero() || time.Since(c.reduceTasking[i]).Seconds() > 10 {
					reply.TaskType = Reduce
					reply.TaskNum = i
					c.reduceTasking[i] = time.Now()
					return nil
				} else {
					reduceDone = false
				}
			}
		}
		if !reduceDone {
		} else {
			break
		}
	}
	reply.TaskType = Done
	c.isDone = true

	return nil
}

func (c *Coordinator) TaskDone(args *TaskArgs, reply *TaskArgs) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	switch args.TaskType {
	case Map:
		c.mapTasked[args.TaskNum] = true
	case Reduce:
		c.reduceTasked[args.TaskNum] = true
	default:
		log.Fatalf("bad finished task %s", args.TaskType)
	}
	return nil
}

func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.isDone {
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

	c.nreduceTasks = nReduce
	c.nmapTasks = len(files)

	for i := 0; i < len(files); i++ {
		c.mapFiles[i] = files[i]
		c.mapTasked[i] = false
		c.reduceTasked[i] = false
	}

	//c.mapTasked = make(map[int]Task)
	//c.mapTasking = make(map[int]Task)
	//c.reduceTasked = make(map[int]Task)
	//c.reduceTasking = make(map[int]Task)

	c.server()
	return &c
}
