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
	"6.824/labgob"
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"sort"
	"strings"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
const (
	CANADIDATE = "CANADIDATE"
	FOLLOWER   = "FOLLOWER"
	LEADER     = "LEADER"
)

const KV_SERVER_REQUEST_TAG = "6.824-LAB3B-KVSERVER-REQUEST"
const KV_PATTERN = "<(?s:(.*?))>"

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

type LogEntry struct {
	Term    int
	Command interface{}
}

type UUIDEXISTS struct {
	// 这是一个空的结构体，相当于一个占位符。
	// 用来实现UUID的集合
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // 服务器当前的的term号
	voteFor     int // 给谁投票了
	status      string
	//entryLog     []LogEntry // log
	entryLog      map[int]LogEntry
	lastLogIndex  int
	lastLogTerm   int
	heartbeat     int
	votedTerm     int
	commited      int
	snapshotIndex int
	snapshotTerm  int
	snapshot      []byte
	applyCh       chan ApplyMsg
	commitLog     []bool
	kvServRequest bool
	kvUUIDSET     map[string]UUIDEXISTS
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.status == LEADER
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	data := rf.getRaftStateData()
	if rf.killed() == true {
		return
	}
	rf.persister.SaveRaftState(data)
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

func (rf *Raft) getRaftStateData() []byte {
	EntryLog := rf.entryLog
	CurrentTerm := rf.currentTerm
	VotedTerm := rf.votedTerm
	VoteFor := rf.voteFor
	UUIDSET := rf.kvUUIDSET
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(EntryLog)
	e.Encode(CurrentTerm)
	e.Encode(VotedTerm)
	e.Encode(VoteFor)
	e.Encode(UUIDSET)
	return w.Bytes()
}

func (rf *Raft) readSnapshot(data []byte) (int, []interface{}) {
	var CommandIndex int
	var xlog []interface{}
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return CommandIndex, xlog
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&CommandIndex) != nil ||
		d.Decode(&xlog) != nil {
		DPrintf("server: %d term:%d decode error!!!\n", rf.me, rf.currentTerm)
		return CommandIndex, xlog
	}
	return CommandIndex, xlog[1:]
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	var EntryLog map[int]LogEntry
	var CurrentTerm int
	var VotedTerm int
	var VoteFor int
	var UUIDSET map[string]UUIDEXISTS
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&EntryLog) != nil ||
		d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VotedTerm) != nil ||
		d.Decode(&VoteFor) != nil ||
		d.Decode(&UUIDSET) != nil {
		DPrintf("server: %d term:%d decode error!!!\n", rf.me, rf.currentTerm)
	} else {
		rf.entryLog = EntryLog
		rf.currentTerm = CurrentTerm
		rf.votedTerm = VotedTerm
		rf.voteFor = VoteFor
		rf.snapshot = rf.persister.ReadSnapshot()
		rf.snapshotIndex, _ = rf.readSnapshot(rf.snapshot)
		rf.kvUUIDSET = UUIDSET
		rf.snapshotTerm = 0
		_, lastIndex := rf.getLogKeys()
		if len(rf.entryLog) == 0 {
			rf.lastLogIndex = rf.snapshotIndex
		} else {
			rf.lastLogIndex = lastIndex + 1
		}
		//sda
		if len(rf.entryLog) == 0 {
			rf.lastLogTerm = 0
		} else {
			rf.lastLogTerm = rf.entryLog[rf.lastLogIndex-1].Term
		}
	}
	fmt.Printf("rf %d restart lastIndex is %d snapshotIndex is %d\n", rf.me, rf.lastLogIndex, rf.snapshotIndex)
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

func recoveryHistory(status []interface{}, database map[string]string) {
	for _, op := range status {
		command, ok := op.(string)
		if ok == false {
			fmt.Printf("op to string error%v\n", op)
		}
		argsMap := make(map[string]string, 3)
		extractArgs(command, KV_PATTERN, argsMap)
		database[argsMap["key"]] = argsMap["value"]
	}

}

func extractArgs(str string, pattern string, argsMap map[string]string) {
	// pattern : "<(?s:(.*?))>"
	reg := regexp.MustCompile(pattern)
	if reg == nil {
		fmt.Println("MustCompile err")
		return
	}
	res := reg.FindAllStringSubmatch(str, -1)
	for _, text := range res {
		kv := strings.Split(text[1], ":")
		argsMap[kv[0]] = kv[1]
	}
}

func (rf *Raft) updateKvOps(log LogEntry, database map[string]string) {
	operation, ok := log.Command.(string)
	if ok == false {
		fmt.Errorf("update kvdatabase failed! log %v\n", log)
		return
	}
	argsMap := make(map[string]string, 3)
	extractArgs(operation, KV_PATTERN, argsMap)
	_, ok = rf.kvUUIDSET[argsMap["uuid"]]
	if argsMap["op"] == "GET" || ok == true {
		return
	}
	switch argsMap["op"] {
	case "PUT":
		database[argsMap["key"]] = argsMap["value"]
	case "APPEND":
		database[argsMap["key"]] += argsMap["value"]
	}
	rf.kvUUIDSET[argsMap["uuid"]] = UUIDEXISTS{}
}

func makeSnapshot(CommittedIndex int, database map[string]string) []byte {
	snapshot := []interface{}{nil}
	for k, v := range database {
		snapshot = append(snapshot, fmt.Sprintf("<key:%s> <value:%s>", k, v))
	}
	//fmt.Printf("make snapshot, snapshot is %v\n", snapshot)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(CommittedIndex)
	e.Encode(snapshot)
	return w.Bytes()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	rf.snapshotIndex = index
	rf.snapshotTerm = rf.entryLog[index-1].Term
	// 这里要做一个判断，如果不似乎lab3b就直接等于snapshot
	var CommittedIndex int
	var HistoryStatus []interface{}
	HistoryDatabase := make(map[string]string)
	if rf.kvServRequest == false {
		rf.snapshot = snapshot
	} else {
		CommittedIndex, HistoryStatus = rf.readSnapshot(snapshot)
		//fmt.Printf("raft server %d read history status %v index is %d\n", rf.me, HistoryStatus, index)
		HistoryDatabase = make(map[string]string, len(HistoryStatus))
		recoveryHistory(HistoryStatus, HistoryDatabase)
	}
	keys, _ := rf.getLogKeys()
	for _, key := range keys {
		if key < index {
			// 在这里要加一条语句
			// Log 的类型是map[int]LogEntry
			// LogEntry是{term,command}
			// 这里对key做了排序了
			if rf.kvServRequest == true {
				rf.updateKvOps(rf.entryLog[key], HistoryDatabase)
			}
			delete(rf.entryLog, key)
			rf.persist()
		}
	}
	if rf.kvServRequest == true {
		rf.snapshot = makeSnapshot(CommittedIndex, HistoryDatabase)
	}
	data := rf.getRaftStateData()
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
	rf.mu.Unlock()

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term          int
	CandidateId   int
	LastLogIndex  int
	LastLogTerm   int
	LastCommitted int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	RecoverIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	UUIDSet           map[string]UUIDEXISTS
}

type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	voteFlag := false
	rf.mu.Lock()
	if rf.status == LEADER && args.Term > rf.currentTerm {
		rf.status = FOLLOWER
		rf.currentTerm = args.Term
		rf.persist()
		DPrintf("%d is no longer a leader because it get a voteRPC from others\n", rf.me)
		// for rf.isLeader   sendHeartBeat
	}
	if (args.Term <= rf.currentTerm) || (args.Term <= rf.votedTerm) || (args.LastCommitted < rf.commited) {
		reply.Term = rf.currentTerm
		voteFlag = false
	} else {
		reply.Term = args.Term
		if args.LastLogTerm < rf.lastLogTerm {
			voteFlag = false
		} else if args.LastLogTerm > rf.lastLogTerm {
			voteFlag = true
		} else {
			if args.LastLogIndex >= rf.lastLogIndex {
				voteFlag = true
			} else {
				voteFlag = false
			}
		}
	}
	if voteFlag == true {
		rf.votedTerm = args.Term
		rf.voteFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
	return
}
func (rf *Raft) InstallSnapshotRPC(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("%d receive install snapshotRPC\n", rf.me)
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if rf.status == LEADER && args.Term > rf.currentTerm {
		rf.status = FOLLOWER
		rf.currentTerm = args.Term
		rf.persist()
		DPrintf("%d is no longer a leader because it get a InstallSnapshotRPC from others\n", rf.me)
		reply.Term = rf.currentTerm
		return
	}
	if args.Term < rf.currentTerm {
		return
	}
	keys, _ := rf.getLogKeys()
	prevLastIndex := rf.snapshotIndex
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	for _, key := range keys {
		if key < rf.snapshotIndex {
			delete(rf.entryLog, key)
		}
	}
	_, lastIndex := rf.getLogKeys()
	if len(rf.entryLog) == 0 {
		rf.lastLogIndex = rf.snapshotIndex
	} else {
		rf.lastLogIndex = lastIndex + 1
	}
	rf.kvUUIDSET = args.UUIDSet
	data := rf.getRaftStateData()
	rf.persister.SaveStateAndSnapshot(data, args.Data)
	_, logs := rf.readSnapshot(args.Data)
	DPrintf("%d commited %d snapIndex %d lastIndex %d\n", rf.me, rf.commited, rf.snapshotIndex, rf.lastLogIndex)
	DPrintf("%d commited %d len logs %d logs%v\n", rf.me, rf.commited, len(logs), logs)
	//fmt.Printf("rf %d receive snapshot, lastIndex is %d commit is %d\n", rf.me, prevLastIndex, rf.commited)
	if prevLastIndex < len(logs) {
		//对比follower之前的l快照和leader发送过来的快照，通过这种方式来提交
		lastCommited := int(math.Max(float64(prevLastIndex), float64(rf.commited)))
		//rf.commited = int(math.Max(math.Min(float64(rf.snapshotIndex), float64(prevLastIndex)), float64(rf.commited)))
		for i := lastCommited + 1; i <= len(logs); i++ {
			applyMsg := ApplyMsg{CommandValid: true, Command: logs[i-1], CommandIndex: i}
			//rand.Seed(time.Now().Unix() + int64(math.Pow(float64(rf.me+100), 3))*1551)
			//timeout := (rand.Intn(10) + 1) * 50
			//time.Sleep(time.Duration(timeout) * time.Millisecond)
			rf.applyCh <- applyMsg
			rf.commited = i
		}
		//DPrintf("%d update its commited, now his log is %v\n", rf.me, rf.entryLog)
	}
	rf.mu.Unlock()

}

func (rf *Raft) AppendEntriesRPC(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if rf.status == LEADER && args.Term > rf.currentTerm {
		rf.status = FOLLOWER
		rf.currentTerm = args.Term
		rf.persist()
		DPrintf("%d is no longer a leader because it get a appendEntriesRPC from others\n", rf.me)
	}
	//DPrintf("%d get heartbeat %d from %d his term is %d leader term is %d \n", rf.me, rf.heartbeat, args.LeaderId, rf.currentTerm, args.Term)
	if len(args.Entries) != 0 {
		reply.Success = rf.canAddNewLog(args)
		if reply.Success == true {
			// 如果这个新的log被接收，则将其加到它的logentries中
			for _, entry := range args.Entries {
				//if !(entry.Term == rf.lastLogTerm && entry.Command == rf.entryLog[rf.lastLogIndex-1].Command) {
				//	rf.addEntryToLog(rf.lastLogIndex, entry)
				//}
				if entry.Command != rf.entryLog[rf.lastLogIndex-1].Command {
					rf.addEntryToLog(rf.lastLogIndex, entry)
				} else if entry.Term != rf.lastLogTerm {
					oldEntry := rf.entryLog[rf.lastLogIndex-1]
					oldEntry.Term = entry.Term
					rf.entryLog[rf.lastLogIndex-1] = oldEntry
				}
			}
			//DPrintf("%d add new entries, current entries is %v\n", rf.me, rf.entryLog)
		} else if args.PrevLogIndex < rf.lastLogIndex && args.Term >= rf.currentTerm {
			rf.removeConflictLogEntry(args.PrevLogIndex)
		} else if args.PrevLogIndex == rf.lastLogIndex && args.PrevLogTerm != rf.lastLogTerm && args.Term >= rf.currentTerm {
			// 如果logEntries冲突，那么就直接删除冲突logEntry，这里每次只删除一个就可以。
			rf.removeConflictLogEntry(args.PrevLogIndex - 1)
		}
		if args.PrevLogTerm != rf.lastLogTerm && args.Term >= rf.currentTerm {
			//i := len(rf.entryLog) - 1
			//for i >= 0 && rf.entryLog[i].Term == rf.lastLogTerm {
			//	i -= 1
			//}
			//reply.RecoverIndex = i
			keys, _ := rf.getLogKeys()
			ind := rf.snapshotIndex
			for i := len(keys); i > 1; i-- {
				if rf.entryLog[keys[i-1]].Term != rf.lastLogTerm {
					ind = keys[i-1]
					break
				}
			}
			reply.RecoverIndex = ind
		} else {
			reply.RecoverIndex = rf.lastLogIndex
		}
	} else {
		rf.heartbeat += 1
		rf.heartbeat %= math.MaxInt32
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.persist()
		}
		reply.Success = true
		if rf.heartbeat%200 == 0 {
			DPrintf("%d get heart beat from %d, his commited is :%d, leader commited is:%d his term is %d, receive term is %d\n", rf.me, args.LeaderId, rf.commited, args.LeaderCommit, rf.currentTerm, args.Term)
		}
	}
	if args.LeaderCommit > rf.commited && rf.lastLogTerm == args.PrevLogTerm && rf.lastLogIndex == args.PrevLogIndex {
		lastCommited := int(math.Max(float64(rf.commited), float64(rf.snapshotIndex)))
		DPrintf("%d lastCommited %d leaderCommited %d lasLogIndex %d\n", rf.me, lastCommited, args.LeaderCommit, rf.lastLogIndex)
		shouldCommited := int(math.Max(math.Min(float64(args.LeaderCommit), float64(rf.lastLogIndex)), float64(lastCommited)))
		for i := lastCommited + 1; i <= shouldCommited; i++ {
			applyMsg := ApplyMsg{CommandValid: true, Command: rf.entryLog[i-1].Command, CommandIndex: i}
			//rand.Seed(time.Now().Unix() + int64(math.Pow(float64(rf.me+100), 3))*1551)
			//timeout := (rand.Intn(10) + 1) * 50
			//time.Sleep(time.Duration(timeout) * time.Millisecond)
			rf.applyCh <- applyMsg
			rf.commited = i
		}
		//DPrintf("%d update its commited, now his log is %v\n", rf.me, rf.entryLog)
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) removeConflictLogEntry(index int) {
	// entry use slice
	//rf.entryLog = rf.entryLog[:index]
	keys, _ := rf.getLogKeys()
	for _, key := range keys {
		if key >= index {
			delete(rf.entryLog, key)
		}
	}
	rf.persist()
	_, lastIndex := rf.getLogKeys()
	if len(rf.entryLog) == 0 {
		rf.lastLogIndex = rf.snapshotIndex
	} else {
		rf.lastLogIndex = lastIndex + 1
	}
	if rf.lastLogIndex > 0 {
		rf.lastLogTerm = rf.entryLog[rf.lastLogIndex-1].Term
	} else {
		rf.lastLogTerm = 0
	}
}

func (rf *Raft) getLogKeys() ([]int, int) {
	j := 0
	keys := make([]int, len(rf.entryLog))
	lastIndex := 0
	for k := range rf.entryLog {
		keys[j] = k
		if k > lastIndex {
			lastIndex = k
		}
		j += 1
	}
	sort.Ints(keys)
	return keys, lastIndex
}

func (rf *Raft) canAddNewLog(args *AppendEntriesArgs) bool {
	// 这个方法用来判断一个新来的logs是否可以被follower所接收
	// 这个方法不需要加锁,因为在外层已经有锁了。
	// DPrintf("follower:%d leader %d follower term %d, leader term %d\n", rf.me, args.LeaderId, rf.currentTerm, args.Term)
	DPrintf("follower:%d leader %d leader last log:%d, follower last log :%d, leader last term:%d, follower last term:%d\n", rf.me, args.LeaderId, args.PrevLogIndex, rf.lastLogIndex, args.PrevLogTerm, rf.lastLogTerm)
	if args.Term < rf.currentTerm {
		return false
	}
	if !(rf.lastLogTerm == args.PrevLogTerm && rf.lastLogIndex == args.PrevLogIndex) {
		return false
	}
	return true

}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshotRPC(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshotRPC", args, reply)
	return ok
}

//
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
//

func (rf *Raft) judgeKvServRequest(command interface{}) {
	request, ok := command.(string)
	if ok == true && strings.Contains(request, KV_SERVER_REQUEST_TAG) {
		rf.mu.Lock()
		rf.kvServRequest = true
		rf.mu.Unlock()
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	// 首先判断这个 service是不是leader，不是直接返回false
	rf.judgeKvServRequest(command)
	rf.mu.Lock()
	if rf.status != LEADER {
		rf.mu.Unlock()
		return -1, -1, false
	}
	rf.mu.Unlock()
	// 如果是leader 则向follower发送log
	index, term, isLeader = rf.logAgreement(command)
	return index, term, isLeader
}

func (rf *Raft) addEntryToLog(index int, entry LogEntry) (int, int) {
	// entry use slice
	//rf.entryLog = rf.entryLog[:index]
	//rf.entryLog = append(rf.entryLog, entry)
	rf.entryLog[index] = entry
	rf.persist()
	//DPrintf("%d add new entryLog %v his current log is %v\n", rf.me, entry, rf.entryLog)
	// entry use slice
	_, lastIndex := rf.getLogKeys()
	if rf.lastLogIndex < lastIndex+1 {
		rf.lastLogIndex = lastIndex + 1
	}
	if rf.lastLogTerm < entry.Term {
		rf.lastLogTerm = entry.Term
	}

	return rf.lastLogIndex, rf.lastLogTerm
}

func (rf *Raft) logAgreement(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	// 首先将command保存到log中
	entry := LogEntry{Term: rf.currentTerm, Command: command}
	entries := []LogEntry{}
	entries = append(entries, entry)
	DPrintf("a new entry %v send to leader %d\n", entries, rf.me)
	//fmt.Printf("a new entry send to leader %d\n", rf.me)
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogTerm:  rf.lastLogTerm,
		PrevLogIndex: rf.lastLogIndex,
		Entries:      entries,
		LeaderCommit: rf.commited}
	var index, term int
	if rf.lastLogIndex > 0 && rf.entryLog[rf.lastLogIndex-1].Command == command {
		index = rf.lastLogIndex
		term = rf.currentTerm
		oldEntry := rf.entryLog[rf.lastLogIndex-1]
		oldEntry.Term = term
		rf.entryLog[rf.lastLogIndex-1] = oldEntry
	} else {
		index, term = rf.addEntryToLog(rf.lastLogIndex, entry)
		//DPrintf("%d add new entries, current entries is %v\n", rf.me, rf.entryLog)
	}
	rf.mu.Unlock()
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	var count int
	mu.Lock()
	count = 1
	mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.mu.Lock()
			go func(x int, args AppendEntriesArgs) {
				rf.mu.Unlock()
				rf.mu.Lock()
				if rf.status != LEADER {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				ok := false
				retry := 0
				for retry < 5 && ok == false {
					ok = rf.sendAppendEntriesRPC(x, &args, &reply)
					cond.Broadcast()
					retry += 1
				}
				if ok == false {
					return
				}
				isLeader := true
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = args.Term
					rf.persist()
					if rf.status == LEADER {
						rf.status = FOLLOWER
						isLeader = false
					}
				}
				rf.mu.Unlock()
				if reply.Success == true {
					mu.Lock()
					count += 1
					mu.Unlock()
				} else {
					// 如果follower的回复为false，说明follower的log和leader的log不一致，
					// 那么就去更新follower的
					if isLeader {
						succ := rf.upDateLogEntries(x, reply.RecoverIndex)
						if succ == true {
							mu.Lock()
							count += 1
							mu.Unlock()
						}
					}
				}
			}(i, args)
		}
	}
	leaderFlag := true
	timeFlag := true
	now := time.Now()
	mu.Lock()
	c := count
	mu.Unlock()
	for leaderFlag && timeFlag && c < (len(rf.peers)+1)/2 {
		time.Sleep(10 * time.Millisecond)
		mu.Lock()
		c = count
		mu.Unlock()
		rf.mu.Lock()
		leaderFlag = rf.status == LEADER
		rf.mu.Unlock()
		if time.Since(now) > 1000*time.Millisecond {
			timeFlag = false
		}
	}
	if leaderFlag == false || timeFlag == false {
		return index, term, leaderFlag
	}
	rf.mu.Lock()
	if rf.lastLogIndex < index {
		rf.lastLogIndex = index
	}
	if rf.lastLogTerm < term {
		rf.lastLogTerm = term
	}
	if rf.status == LEADER {
		DPrintf("success achieve agree on index %d,count is %d, half is %d all is %d\n", index, count, (len(rf.peers)+1)/2, len(rf.peers))
		//fmt.Printf("%d success achieve agree on index %d entry:%v\n", rf.me, index, entry)
		lastCommited := int(math.Max(float64(rf.commited), float64(rf.snapshotIndex)))
		DPrintf("rf.commited: %d\n", lastCommited)
		//rf.commited = rf.upDateCommited(lastCommited, index)
		for i := lastCommited + 1; i <= index; i++ {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.entryLog[i-1].Command,
				CommandIndex: i}
			//rand.Seed(time.Now().Unix() + int64(math.Pow(float64(rf.me+100), 3))*1551)
			//timeout := (rand.Intn(10) + 1) * 50
			//time.Sleep(time.Duration(timeout) * time.Millisecond)
			//DPrintf("send msg %v to ch\n", applyMsg)
			rf.applyCh <- applyMsg
			rf.commited = i
		}
		//DPrintf("%d update its commited, now his log is %v\n", rf.me, rf.entryLog)
	}
	defer rf.mu.Unlock()
	return index, term, rf.status == LEADER
}

func (rf *Raft) upDateCommited(last int, index int) int {
	if index > len(rf.commitLog) {
		for i := len(rf.commitLog); i < index; i++ {
			rf.commitLog = append(rf.commitLog, false)
		}
	}
	rf.commitLog[index-1] = true
	newCommited := last
	for newCommited < len(rf.commitLog) && rf.commitLog[newCommited] == true {
		newCommited += 1
	}
	return newCommited
}

func (rf *Raft) sendInstallSnapshotRPCToFollower(x int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedTerm:  rf.snapshotTerm,
		LastIncludedIndex: rf.snapshotIndex,
		Data:              rf.snapshot,
		UUIDSet:           rf.kvUUIDSET}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	ok := false
	retry := 0
	for retry < 1 && ok == false {
		ok = rf.sendInstallSnapshotRPC(x, &args, &reply)
		retry += 1
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
		if rf.status == LEADER {
			rf.status = FOLLOWER
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) upDateLogEntries(x int, recoverIndex int) bool {
	// 先向follower发送一个心跳同步一下信息
	DPrintf("%d's log is not the same with leader %d, so update his log\n", x, rf.me)
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogTerm:  rf.lastLogTerm,
		PrevLogIndex: rf.lastLogIndex,
		LeaderCommit: rf.commited}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := false
	retry := 0
	for retry < 1 && ok == false {
		ok = rf.sendAppendEntriesRPC(x, &args, &reply)
		retry += 1
	}
	if ok == false {
		return false
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
		if rf.status == LEADER {
			rf.status = FOLLOWER
			DPrintf("%d is no longer a leader because it get a heartbeat from others\n", rf.me)
			rf.mu.Unlock()
			return false
		}
	}
	rf.mu.Unlock()
	succ := false
	rf.mu.Lock()
	index := int(math.Max(float64(recoverIndex), float64(rf.snapshotIndex)))
	log := rf.entryLog
	keys, _ := rf.getLogKeys()
	rf.mu.Unlock()
	for succ == false {
		rf.mu.Lock()
		if rf.status != LEADER {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		// send installsnapshotIndex
		if index <= rf.snapshotIndex {
			DPrintf("index %d,snapIndex %d\n", index, rf.snapshotIndex)
			rf.sendInstallSnapshotRPCToFollower(x)
		}
		rf.mu.Lock()
		if rf.status != LEADER {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		// entry use slice
		entries := []LogEntry{}
		rf.mu.Lock()
		for _, key := range keys {
			if key >= index {
				entries = append(entries, log[key])
			}
		}
		rf.mu.Unlock()
		DPrintf("leader %d send partial %v entries to %d\n", rf.me, entries, x)
		//entries := log[index:]v c
		prevTerm := 0
		if index > 0 {
			prevTerm = log[index-1].Term
		}
		rf.mu.Lock()
		args = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogTerm:  prevTerm,
			PrevLogIndex: index,
			Entries:      entries,
			LeaderCommit: rf.commited}
		rf.mu.Unlock()
		reply = AppendEntriesReply{}

		ok = false
		retry = 0
		for retry < 1 && ok == false {
			ok = rf.sendAppendEntriesRPC(x, &args, &reply)
			retry += 1
		}
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.persist()
			if rf.status == LEADER {
				rf.status = FOLLOWER
			}
		}
		rf.mu.Unlock()
		if reply.Success == false && ok == true {
			DPrintf("log update index:%d failed\n", index)
			rf.mu.Lock()
			index = int(math.Max(float64(reply.RecoverIndex), float64(rf.snapshotIndex)))
			rf.mu.Unlock()
		}
		if reply.Success == true {
			succ = true
		}
	}
	return succ
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) judgeLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.status == LEADER
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogTerm: rf.lastLogTerm, PrevLogIndex: rf.lastLogIndex, LeaderCommit: rf.commited}
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.mu.Lock()
			go func(x int, args AppendEntriesArgs) {
				rf.mu.Unlock()
				rf.mu.Lock()
				if rf.status != LEADER {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				ok := false
				retry := 0
				//DPrintf("%d send heartbeat to %d\n", rf.me, x)
				for retry < 1 && ok == false {
					ok = rf.sendAppendEntriesRPC(x, &args, &reply)
					retry += 1
				}
				if ok == false {
					return
				}
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = args.Term
					rf.persist()
					if rf.status == LEADER {
						rf.status = FOLLOWER
						DPrintf("%d is no longer a leader because it get a heartbeat from others\n", rf.me)
					}
				}
				rf.mu.Unlock()
			}(i, args)
		}
	}
}

func (rf *Raft) judgeHeartBeat(timeout int) bool {
	rf.mu.Lock()
	stHeartbeatNum := rf.heartbeat
	rf.mu.Unlock()
	time.Sleep(time.Duration(timeout) * time.Millisecond)
	rf.mu.Lock()
	edHeartbeatNum := rf.heartbeat
	rf.mu.Unlock()
	return stHeartbeatNum != edHeartbeatNum
}

func (rf *Raft) election(timeout int) bool {
	rf.mu.Lock()
	stHeartbeatNum := rf.heartbeat
	rf.currentTerm += 1
	if rf.voteFor >= rf.currentTerm {
		rf.mu.Unlock()
		return false
	}
	//DPrintf("%d start one election his term is %d lastlogterm is %d lastlogindex is %d\n", rf.me, rf.currentTerm, rf.lastLogTerm, rf.lastLogIndex)
	rf.voteFor = rf.me
	rf.votedTerm = rf.currentTerm
	rf.persist()
	rf.status = CANADIDATE
	args := RequestVoteArgs{Term: rf.currentTerm,
		CandidateId:   rf.me,
		LastLogIndex:  rf.lastLogIndex,
		LastLogTerm:   rf.lastLogTerm,
		LastCommitted: rf.commited}
	rf.mu.Unlock()
	mu := sync.Mutex{}
	mu.Lock()
	count := 1
	mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.mu.Lock()
			go func(x int, args RequestVoteArgs) {
				rf.mu.Unlock()
				rf.mu.Lock()
				edHeartbeatNum := rf.heartbeat
				rf.mu.Unlock()
				if stHeartbeatNum != edHeartbeatNum {
					return
				}
				reply := RequestVoteReply{}
				ok := false
				retry := 0
				for retry < 1 && ok == false {
					ok = rf.sendRequestVote(x, &args, &reply)
					retry += 1
				}
				if ok == false {
					return
				}
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					if rf.status == LEADER {
						rf.status = CANADIDATE
					}
					rf.currentTerm = reply.Term
					rf.persist()
				}
				rf.mu.Unlock()
				if reply.VoteGranted == true {
					mu.Lock()
					count += 1
					mu.Unlock()
				}
			}(i, args)
		}
	}
	//然后休眠一段时间
	//time.Sleep(time.Duration(timeout)*time.Millisecond)
	now := time.Now()
	finshed := false
	for time.Since(now) < time.Duration(timeout)*time.Millisecond {
		rf.mu.Lock()
		edHeartbeatNum := rf.heartbeat
		rf.mu.Unlock()
		if stHeartbeatNum != edHeartbeatNum {
			finshed = true
			break
		}
		mu.Lock()
		if count > len(rf.peers)/2 {
			mu.Unlock()
			rf.mu.Lock()
			if rf.status == CANADIDATE {
				rf.status = LEADER
			}
			DPrintf("%d become a new leader ,term is %d\n", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			finshed = true
			break

		}
		mu.Unlock()
	}
	return finshed
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// 这里写一下大体的思路，首先是判断是不是这个服务器是不是leader，如果是leader那么就向除自己之外的其他服务器每秒
		// 发送10次心跳，来巩固自己的地位。
		// 如果这个服务器是follower，那么首先要判断它是否在一定时间内没接受到心跳，如果接收到心跳就继续判断，否则就开始选举
		heartbeat := 150
		rand.Seed(int64(math.Pow(float64(rf.me+100), 3)) * 1551)
		timeout := (rand.Intn(10)+1)*50 + 2*heartbeat
		if rf.judgeLeader() == true {
			// 如果是leader就定期发送心跳
			rf.sendHeartBeat()
			time.Sleep(time.Duration(heartbeat) * time.Millisecond)
		} else {
			// 如果不是leader就要先判断一段时间内是否接收到心跳，如果收到心跳就继续做follower
			if rf.judgeHeartBeat(2*heartbeat) == false {
				for rf.election(timeout) == false {
					rf.mu.Lock()
					stHeartbeatNum := rf.heartbeat
					rf.mu.Unlock()
					rand.Seed(time.Now().Unix() + int64(math.Pow(float64(rf.me+100), 3))*1551)
					timeout = (rand.Intn(10)+1)*50 + 2*heartbeat
					//DPrintf("%d timeout is %d seed is %d\n", rf.me, timeout, time.Now().Unix())
					rf.mu.Lock()
					edHeartbeatNum := rf.heartbeat
					rf.mu.Unlock()
					if stHeartbeatNum != edHeartbeatNum {
						break
					}
				}
			}
		}
	}
	DPrintf("%d killed\n", rf.me)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	DPrintf("%d start make\n", rf.me)
	rf.heartbeat = 0
	rf.voteFor = -1
	rf.votedTerm = -1
	rf.status = FOLLOWER
	rf.lastLogTerm = 0
	rf.currentTerm = 0
	rf.lastLogIndex = 0
	rf.commited = 0
	rf.snapshotIndex = 0
	rf.snapshotTerm = 0
	//rf.entryLog = []LogEntry{}
	rf.entryLog = make(map[int]LogEntry, 1000)
	rf.commitLog = []bool{}
	rf.applyCh = make(chan ApplyMsg)
	rf.kvServRequest = false
	rf.kvUUIDSET = make(map[string]UUIDEXISTS, 1000)
	rf.mu.Unlock()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	// 接收 applyMsg
	go func() {
		for applyMsg := range rf.applyCh {
			//rand.Seed(time.Now().Unix() + int64(math.Pow(float64(rf.me+100), 3))*1551)
			//timeout := (rand.Intn(5) + 1) * 200
			//time.Sleep(time.Duration(timeout) * time.Millisecond)
			applyCh <- applyMsg
			//DPrintf("%d get commit msg %v\n", rf.me, applyMsg)
		}
	}()
	return rf
}
