package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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
type TaskType int

const (
	Map    TaskType = 1
	Reduce TaskType = 2
	Done   TaskType = 3
)

// 一个worker要去请求coordinator来给它一个task
type RPCArgs struct{}

// coordinator响应返回给你的task,你需要知道该task是什么类型
type RPCReply struct {
	TaskType     TaskType // task类型
	TaskNum      int      // 编号
	NReduceTasks int
	MapFile      string
	NMapTasks    int
}

// 所传参数就是为了说，编号为哪个的Task已经完成了
type TaskArgs struct {
	TaskType TaskType
	TaskNum  int
}

// worker通知coordinator，这个task它搞定了
type TaskReply struct{}

//type RPCArgs struct {
//	TaskInfo Task
//}
//
//type RPCReply struct {
//	TaskInfo Task
//}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
