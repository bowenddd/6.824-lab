package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

const (
	GET    = "GET"
	PUT    = "PUT"
	APPEND = "APPEND"
)
const KV_PATTERN = "<(?s:(.*?))>"
const PREFIX = "6.824-LAB3B-KVSERVER-REQUEST"

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation int
	Args      interface{}
}

type HandleRes struct {
	Reply interface{}
	UUID  string
}

type DataPersist struct {
	Committed int               `json:"committed"`
	Database  map[string]string `json:"database"`
}

type UUIDEXIST struct {
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	// Your definitions here.
	database     map[string]string //kv数据库
	opres        map[int]HandleRes //执行结果
	committedCmd map[string]int
	committed    int
	isLeader     bool
	//persistPath  string
	persist       *raft.Persister
	historyStatus []interface{}
	uuidSet       map[string]UUIDEXIST
}

func (kv *KVServer) GetOpResMapKeys() []int {
	ind := 0
	keys := make([]int, len(kv.opres))
	for key := range kv.opres {
		keys[ind] = key
		ind += 1
	}
	//sort.Ints(keys)
	return keys
}

func contains(keys []int, key int) bool {
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}

func (kv *KVServer) judgeHandleSucc(index int) bool {
	kv.mu.Lock()
	keys := kv.GetOpResMapKeys()
	kv.mu.Unlock()
	// 是不是还要加一个timeflag 或许不用判断超时？
	now := time.Now()
	timeFlag := true
	for contains(keys, index) == false && timeFlag == true {
		if kv.killed() == true {
			timeFlag = false
			break
		}
		time.Sleep(2 * time.Millisecond)
		if kv.killed() == true {
			timeFlag = false
			break
		}
		kv.mu.Lock()
		keys = kv.GetOpResMapKeys()
		kv.mu.Unlock()
		if time.Since(now) > 2000*time.Millisecond {
			timeFlag = false
		}
	}
	return timeFlag
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 需要在服务器端加循环吗？我觉得没必要，如果客户端在一段时间内没有收到，回复那么重发就可以了
	cmd := fmt.Sprintf("%s <op:%s> <key:%s> <uuid:%s>", PREFIX, GET, args.Key, args.UUID)
	index, _, isLeader := kv.rf.Start(cmd)
	kv.mu.Lock()
	kv.isLeader = isLeader
	kv.mu.Unlock()
	if isLeader == false {
		reply.Value = ""
		reply.Err = ErrWrongLeader
		return
	}

	// 在这里有可能出现一种情况，就是在handleOperation中在opres中加入处理结果，在下面取
	// 处理结果，这个操作不是原子操作，有可能出现不一致性。
	if kv.judgeHandleSucc(index) == true {
		kv.mu.Lock()
		indexRes, ok := kv.opres[index].Reply.(GetReply)
		indexResUUid := kv.opres[index].UUID
		kv.mu.Unlock()
		if args.UUID == indexResUUid && ok == true {
			reply.Value = indexRes.Value
			reply.Err = indexRes.Err
		} else {
			//fmt.Printf(" get information conflict: UUID %v, indexResUUid %v, index %v, ok %v\n", args.UUID, indexResUUid, index, ok)
			reply.Value = ""
			reply.Err = ErrNoAgreement
		}
	} else {
		reply.Value = ""
		reply.Err = ErrNoAgreement
		//fmt.Printf("get %d handle false\n", index)
	}
}

// putappend 的操作与get的操作类似，但是要把put和append进行区分，也就是说在server端，put和append复用了
// 同一个rpc
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var cmd string
	switch args.Op {
	case "Put":
		cmd = fmt.Sprintf("%s <op:%s> <key:%s> <value:%s> <uuid:%s>", PREFIX, PUT, args.Key, args.Value, args.UUID)
	case "Append":
		cmd = fmt.Sprintf("%s <op:%s> <key:%s> <value:%s> <uuid:%s>", PREFIX, APPEND, args.Key, args.Value, args.UUID)
	}
	index, _, isLeader := kv.rf.Start(cmd)
	kv.mu.Lock()
	kv.isLeader = isLeader
	kv.mu.Unlock()
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("kv %d  index %d cmd %s\n", kv.me, index, cmd)
	if kv.judgeHandleSucc(index) == true {
		kv.mu.Lock()
		indexRes, ok := kv.opres[index].Reply.(PutAppendReply)
		indexResUUid := kv.opres[index].UUID
		kv.mu.Unlock()
		if args.UUID == indexResUUid && ok == true {
			reply.Err = indexRes.Err
		} else {
			//fmt.Printf("get information conflict: UUID %v, indexResUUid %v, index %v, ok %v\n", args.UUID, indexResUUid, index, ok)
			reply.Err = ErrNoAgreement
		}
	} else {
		reply.Err = ErrNoAgreement
		//fmt.Printf("get %d handle false\n", index)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	defer fmt.Println(kv.me, " killed")
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//func (kv *KVServer) persist(fileName string) {
//	dataPersist := DataPersist{Committed: kv.committed, Database: kv.database}
//	jsonPersist, err := json.Marshal(dataPersist)
//	if err != nil {
//		fmt.Errorf("json marshal error %v", dataPersist)
//		return
//	}
//	err = ioutil.WriteFile(fileName, jsonPersist, 0777)
//	if err != nil {
//		log.Fatal(err)
//	}
//}

//func (kv *KVServer) readPersistStatus(fileName string) {
//	_, err := os.Stat(fileName)
//	if os.IsNotExist(err) {
//		return
//	}
//	content, err := ioutil.ReadFile(fileName)
//	if err != nil {
//		fmt.Errorf("open file %s error", fileName)
//	}
//	persistData := DataPersist{}
//	json.Unmarshal(content, &persistData)
//	kv.mu.Lock()
//	kv.committed = persistData.Committed
//	kv.database = persistData.Database
//	fmt.Printf("committed %v database %v\n", kv.committed, kv.database)
//	kv.mu.Unlock()
//}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	fmt.Printf("start kv %d\n", me)
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.mu.Lock()
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	kv.database = make(map[string]string, 1000)
	kv.opres = make(map[int]HandleRes, 1000)
	kv.committedCmd = make(map[string]int, 1000)
	kv.committed = 0
	kv.isLeader = false
	kv.persist = persister
	kv.historyStatus = []interface{}{nil}
	kv.uuidSet = make(map[string]UUIDEXIST, 1000)
	// kv.persistPath = fmt.Sprintf("/Users/dengbowen/go/src/6.824/src/kvraft/persist/kv-%d-persistdata.json", kv.me)
	kv.readSnapshot()
	recoveryHistory(kv.historyStatus[1:], kv.database)
	kv.readPersist(persister.ReadRaftState())
	//fmt.Printf("%d restart and read from snapshot... committed is %d database is %v historyStatus is %v uuidset is%v\n", kv.me, kv.committed, kv.database, kv.historyStatus, kv.uuidSet)
	kv.mu.Unlock()
	//kv.readPersistStatus(kv.persistPath)
	go kv.handleOperation()
	go kv.createSnapshot()
	return kv
}

func (kv *KVServer) readSnapshot() {
	snapshot := kv.persist.ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	var CommittedIndex int
	var HistortStatus []interface{}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&CommittedIndex) != nil ||
		d.Decode(&HistortStatus) != nil {
		DPrintf("kvserver: %d decode error!!!\n", kv.me)
	} else {
		kv.committed = CommittedIndex
		kv.historyStatus = HistortStatus
	}
}

func recoveryHistory(status []interface{}, database map[string]string) {
	for _, op := range status {
		command, ok := op.(string)
		if ok == false {
			fmt.Printf("op to string error%v\n", op)
		}
		argsMap := make(map[string]string)
		extractArgs(command, KV_PATTERN, argsMap)
		database[argsMap["key"]] = argsMap["value"]
	}

}

func (kv *KVServer) recoveryFromSnapshot() {
	kv.mu.Lock()
	kv.database = make(map[string]string, 1000)
	kv.readSnapshot()
	recoveryHistory(kv.historyStatus[1:], kv.database)
	kv.readPersist(kv.persist.ReadRaftState())
	//fmt.Printf("%d recovery from snapshot databse is %v,uuidset is %v\n", kv.me, kv.database, kv.uuidSet)
	kv.mu.Unlock()
}

func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	var EntryLog map[int]raft.LogEntry
	var CurrentTerm int
	var VotedTerm int
	var VoteFor int
	var UUIDSET map[string]UUIDEXIST
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&EntryLog) != nil ||
		d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VotedTerm) != nil ||
		d.Decode(&VoteFor) != nil ||
		d.Decode(&UUIDSET) != nil {
		DPrintf("server: %d decode error!!!\n", kv.me)
	} else {
		kv.uuidSet = UUIDSET
	}
}

func (kv *KVServer) createSnapshot() {
	oldCommitted := 0
	for kv.killed() == false {
		if kv.persist.RaftStateSize() < kv.maxraftstate*10 {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		CommittedIndex := kv.committed
		histortyStatus := kv.historyStatus
		kv.mu.Unlock()
		if oldCommitted == CommittedIndex {
			//fmt.Printf("oldCommitted %d == committedIndex %d\n", oldCommitted, CommittedIndex)
			time.Sleep(5 * time.Millisecond)
			continue
		}
		oldCommitted = CommittedIndex
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(CommittedIndex)
		e.Encode(histortyStatus)
		//fmt.Printf("kv server %d create snapshot committedIndex is %d old status is %v\n", kv.me, CommittedIndex, kv.historyStatus)
		kv.rf.Snapshot(CommittedIndex, w.Bytes())
		kv.mu.Lock()
		kv.readSnapshot()
		fmt.Printf("kv server %d create snapshot newIndex is %d new status is %v\n", kv.me, kv.committed, kv.historyStatus)
		kv.mu.Unlock()
	}

}

func (kv *KVServer) updateUUIDSet(applyMsg raft.ApplyMsg) {
	if applyMsg.CommandValid == false {
		return
	}
	argsMap := make(map[string]string, 4)
	command := applyMsg.Command.(string)
	extractArgs(command, KV_PATTERN, argsMap)
	if argsMap["op"] == "APPEND" {
		return
	}
	kv.uuidSet[argsMap["uuid"]] = UUIDEXIST{}
	fmt.Printf("update uuid set index is %d uuid is %s\n", applyMsg.CommandIndex, argsMap["uuid"])

}

func (kv *KVServer) handleOperation() {
	kv.mu.Lock()
	oldInd := kv.committed
	kv.mu.Unlock()
	for applyMsg := range kv.applyCh {
		//if kv.killed() == true {
		//	break
		//}
		if applyMsg.CommandIndex != oldInd+1 {
			kv.recoveryFromSnapshot()
		}
		oldInd = applyMsg.CommandIndex
		//fmt.Println(kv.me, " index:", applyMsg.CommandIndex, "command", applyMsg.Command)
		kv.mu.Lock()
		_, finishFlag := kv.opres[applyMsg.CommandIndex]
		committed := kv.committed
		//uuidExistFlag := kv.judgeUUIDExists(applyMsg)
		kv.mu.Unlock()
		if applyMsg.CommandValid == false || finishFlag == true || committed >= applyMsg.CommandIndex {
			if committed >= applyMsg.CommandIndex {
				//fmt.Printf("committed %d >= applyMsg.CommandiNdex %d\n", committed, applyMsg.CommandIndex)
				kv.mu.Lock()
				kv.updateUUIDSet(applyMsg)
				kv.mu.Unlock()
			}
			continue
		}
		DPrintf("get msg %v\n", applyMsg)
		command, ok := applyMsg.Command.(string)
		//if ok == false {
		//	fmt.Printf("%v\n", applyMsg)
		//}
		index := applyMsg.CommandIndex
		argsMap := make(map[string]string, 4)
		extractArgs(command, KV_PATTERN, argsMap)
		op := argsMap["op"]
		key := argsMap["key"]
		uuid := argsMap["uuid"]
		kv.mu.Lock()
		ind, ok := kv.committedCmd[uuid]
		kv.mu.Unlock()
		if ok == true && op != GET {
			kv.mu.Lock()
			kv.opres[index] = kv.opres[ind]
			kv.mu.Unlock()
			//fmt.Printf("index %d ind %d op:%s key %s value %v uuid %s\n", index, ind, op, key, argsMap["value"], uuid)
			continue
		}
		kv.mu.Lock()
		_, ok = kv.uuidSet[uuid]
		kv.mu.Unlock()

		handleRes := HandleRes{UUID: uuid}
		switch op {
		case GET:
			getReply := GetReply{}
			kv.mu.Lock()
			kv.handleGetOperation(key, &getReply)
			handleRes.Reply = getReply
			kv.opres[index] = handleRes
			if kv.isLeader == true {
				fmt.Println(kv.me, " index:", index, "op:", op, "key:", key, "value:", getReply.Value, "time:", strings.Split(uuid, "-")[len(strings.Split(uuid, "-"))-1], kv.database)
			}
			kv.mu.Unlock()
		case PUT, APPEND:
			if ok == false {
				value := argsMap["value"]
				putAppendReply := PutAppendReply{}
				kv.mu.Lock()
				kv.handlePutAppendOperation(op, key, value, &putAppendReply)
				handleRes.Reply = putAppendReply
				kv.opres[index] = handleRes
				if kv.isLeader == true {
					fmt.Println(kv.me, " index:", index, "op:", op, "key:", key, "value:", argsMap["value"], "time:", strings.Split(uuid, "-")[len(strings.Split(uuid, "-"))-1], kv.database)
				}
				kv.uuidSet[uuid] = UUIDEXIST{}
				kv.mu.Unlock()
			}
		}
		kv.mu.Lock()
		if kv.committed < index {
			kv.committed = index
		}
		kv.committedCmd[uuid] = index
		//kv.persist(kv.persistPath)
		kv.mu.Unlock()
	}
}

func (kv *KVServer) handleGetOperation(key string, reply *GetReply) {
	// 这里要对ErrorNoKey加一个判断
	value, ok := kv.database[key]
	reply.Value = value
	if ok == true {
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) handlePutAppendOperation(op string, key string, value string, reply *PutAppendReply) {
	switch op {
	case PUT:
		kv.database[key] = value
	case APPEND:
		kv.database[key] += value
	}
	//if kv.isLeader == true {
	//	fmt.Printf("%v putappend op %v key %v value %v result %v\n", kv.me, op, key, value, kv.database[key])
	//}
	reply.Err = OK
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

// 正则提取内容
// (?<=<)(.+?)(?=>)
