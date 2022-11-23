package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Status int

const (
	Follower Status = iota
	Candidate
	Leader
)

type VoteStatus int

const (
	Good VoteStatus = iota
	Killed
	Expired
	Voted
)

type AppendLogStatus int

const (
	AppendGood AppendLogStatus = iota
	AppendOverTime
	AppendKill
)

var HeartTimeout = 120 * time.Millisecond

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // 服务器已知最新的任期
	votedFor    int // 当前的任期把票投给了谁
	log         []LogEntry
	commitIndex int           // 已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int           // 被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
	nextIndex   []int         // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	matchIndex  []int         // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）
	role        Status        // 节点状态
	overtime    time.Duration // 超时时间
	timer       *time.Ticker  // 节点计时器
	voteCount   int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int        // 领导人的任期
	LeaderId     int        // 领导人ID
	PrevLogIndex int        // 预计要从哪里追加的index，因此每次要比当前的len(logs)多1 args初始化为：rf.nextIndex[i] - 1
	PrevLogTerm  int        // 紧邻新日志条目之前的那个日志条目的任期
	Entries      []LogEntry // 预计存储的日志（为空时就是心跳连接）
	LeaderCommit int        // 领导人的已知已提交的最高的日志条目的索引 leader的commit index指的是最后一个被大多数机器都复制的日志Index
}

type AppendEntriesReply struct {
	Term         int  // 当前任期，对于领导人而言 它会更新自己的任期 leader的term可能是过时的，此时收到的Term用于更新他自己
	Success      bool // 如果跟随者与Args所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了才会追加新日志，不匹配false
	AppendStatus AppendLogStatus
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = int(rf.currentTerm)
	if rf.role == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 需要竞选的人的任期
	CandidateId  int // 需要竞选的人的Id
	LastLogIndex int // 竞选人日志条目最后索引
	LastLogTerm  int // 候选人最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 投票方的term，如果竞选者比自己还低就改为这个
	VoteGranted bool // 是否投票给了该竞选人
	VoteStatus  VoteStatus
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		reply.VoteStatus = Killed
		reply.Term = -1
		reply.VoteGranted = false
		return
	}
	if args.Term < rf.currentTerm {
		reply.VoteStatus = Expired
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.role = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	if rf.votedFor == -1 {
		currentLogIndex := len(rf.log) - 1
		currentLogTerm := 0
		if currentLogIndex >= 0 {
			currentLogTerm = rf.log[currentLogIndex].Term
		}
		if args.LastLogIndex < currentLogIndex || args.LastLogTerm < currentLogTerm {
			reply.VoteStatus = Expired
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}
		rf.votedFor = args.CandidateId
		reply.VoteStatus = Good
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.timer.Reset(rf.overtime)
	} else {
		reply.VoteStatus = Voted
		reply.VoteGranted = false
		if rf.votedFor != args.CandidateId {
			return
		} else {
			rf.role = Follower
		}
		rf.timer.Reset(rf.overtime)
	}
	return
}

//func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
//	//rf.mu.Lock()
//	//defer rf.mu.Unlock()
//	//if rf.killed() {
//	//	reply.AppendStatus = AppendKill
//	//	reply.Term = -1
//	//	reply.Success = false
//	//	return
//	//}
//	//if args.Term < rf.currentTerm {
//	//	reply.AppendStatus = AppendOverTime
//	//	reply.Term = rf.currentTerm
//	//	reply.Success = false
//	//	return
//	//}
//	//rf.currentTerm = args.Term
//	//rf.votedFor = args.LeaderId
//	//rf.role = Follower
//	//rf.timer.Reset(rf.overtime)
//	//reply.AppendStatus = AppendGood
//	//reply.Term = rf.currentTerm
//	//reply.Success = true
//	//return
//}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteCount *int) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//if !ok {
	//	if rf.killed() {
	//		return false
	//	}
	//	ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	//}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		return false
	}
	switch reply.VoteStatus {
	case Expired:
		{
			rf.role = Follower
			rf.timer.Reset(rf.overtime)
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
			}
		}
	case Good, Voted:
		{
			if reply.VoteGranted && reply.Term == rf.currentTerm && *voteCount <= (len(rf.peers)/2) {
				*voteCount++
				fmt.Printf("%d 收到选票 %d\n", rf.me, args.CandidateId)
			}
			if *voteCount >= (len(rf.peers)/2 + 1) {
				*voteCount = 0
				if rf.role == Leader {
					return ok
				}
				rf.role = Leader
				fmt.Printf("%d 变为Leader\n", rf.me)
				rf.nextIndex = make([]int, len(rf.peers))
				for i, _ := range rf.nextIndex {
					rf.nextIndex[i] = len(rf.log) + 1
				}
				rf.timer.Reset(HeartTimeout)
			}
		}
	case Killed:
		return false
	}
	return ok
}

//func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
//	if rf.killed() {
//		return false
//	}
//	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
//	if !ok {
//		if rf.killed() {
//			return false
//		}
//		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
//	}
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	switch reply.AppendStatus {
//	case AppendKill:
//		{
//			return false
//		}
//	case AppendGood:
//		{
//			return true
//		}
//	case AppendOverTime:
//		{
//			rf.role = Follower
//			rf.votedFor = -1
//			rf.timer.Reset(rf.overtime)
//			rf.currentTerm = reply.Term
//		}
//	}
//	return ok
//}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	//rf.mu.Lock()
	//rf.timer.Stop()
	//rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			switch rf.role {
			case Follower:
				rf.role = Candidate
				fmt.Printf("%d 超时了,变成Candidate\n", rf.me)
				fallthrough
			case Candidate:
				rf.currentTerm += 1
				rf.votedFor = rf.me
				voteCount := 1
				rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
				rf.timer.Reset(rf.overtime)
				for i := 0; i < len(rf.peers); i++ {
					if rf.me == i {
						continue
					}
					voteargs := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.log) - 1,
						LastLogTerm:  0,
					}
					if len(rf.log) > 0 {
						voteargs.LastLogTerm = rf.log[len(rf.log)-1].Term
					}
					voteReply := RequestVoteReply{}
					go rf.sendRequestVote(i, &voteargs, &voteReply, &voteCount)
				}
			case Leader:
				//rf.timer.Reset(HeartTimeout)
				//for i := 0; i < len(rf.peers); i++ {
				//	if rf.me == i {
				//		continue
				//	}
				//	appendEntriesArgs := AppendEntriesArgs{
				//		Term:         rf.currentTerm,
				//		LeaderId:     rf.me,
				//		PrevLogIndex: 0,
				//		PrevLogTerm:  0,
				//		Entries:      nil,
				//		LeaderCommit: rf.commitIndex,
				//	}
				//	appendEntrieReply := AppendEntriesReply{}
				//	go rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntrieReply)
				//}
			}
			rf.mu.Unlock()
		}
		fmt.Printf("%d 状态是 %d, term 是 %d\n", rf.me, rf.role, rf.currentTerm)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = 0
	//rf.logs = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.role = Follower
	rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
	rf.timer = time.NewTicker(rf.overtime)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
