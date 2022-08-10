package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"fmt"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
	Key      string
	Value    string
	UUID     string
	MoveArgs interface{}
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	opReply       map[int]OpReply
	msgIndex      int               //一个单调递增的序列号用于按序处理raft中达成共识的操作
	uuidSet       map[string]int    //一个map用于从uuid->index的映射
	kvMap         map[string]string //用于存储键值对
	mck           *shardctrler.Clerk
	latestConfig  shardctrler.Config
	managedShards map[int]Exists
	leader        bool
}

type OpReply struct {
	Type  string
	Reply interface{}
	UUID  string
}

type Exists struct {
	State string
}

func (kv *ShardKV) Agreement(index int, op Op) bool {
	kv.mu.Lock()
	reply, ok := kv.opReply[index]
	kv.mu.Unlock()
	now := time.Now()
	for ok == false && time.Since(now) < AgreementMaxTime {
		time.Sleep(AgreementWaitTime)
		kv.mu.Lock()
		reply, ok = kv.opReply[index]
		kv.mu.Unlock()
	}
	if ok == false {
		return false
	}
	if op.Type != reply.Type || op.UUID != reply.UUID {
		return false
	}
	return true
}

func (kv *ShardKV) SendOpToRaft(op Op) (bool, Err, OpReply) {
	index, _, leader := kv.rf.Start(op)
	kv.mu.Lock()
	kv.leader = leader
	kv.mu.Unlock()
	if leader == false {
		return false, ErrWrongLeader, OpReply{}
	}
	if kv.Agreement(index, op) == false {
		return false, ErrNoAgreement, OpReply{}
	}
	kv.mu.Lock()
	opReply := kv.opReply[index]
	kv.mu.Unlock()
	return true, OK, opReply
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	managedShards := kv.managedShards
	kv.mu.Unlock()
	ok := detectCorrectServer(args.Key, managedShards)
	if ok == false {
		reply.Err = ErrWrongGroup
		return
	}
	op := Op{
		Type: GET,
		Key:  args.Key,
		UUID: args.UUID}
	ok, err, opReply := kv.SendOpToRaft(op)
	if ok == false {
		reply.Err = err
		return
	}
	getReply, ok := opReply.Reply.(GetReply)
	if ok == false {
		DPrintf("change type from opReply to getReply error!")
		reply.Err = ErrChangeType
		return
	}
	reply.Value = getReply.Value
	reply.Err = getReply.Err
	return

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	managedShards := kv.managedShards
	kv.mu.Unlock()
	ok := detectCorrectServer(args.Key, managedShards)
	if ok == false {
		reply.Err = ErrWrongGroup
		return
	}
	op := Op{
		Type:  args.Op,
		Key:   args.Key,
		Value: args.Value,
		UUID:  args.UUID,
	}
	ok, err, opReply := kv.SendOpToRaft(op)
	if ok == false {
		reply.Err = err
		return
	}
	putAppendReply, ok := opReply.Reply.(PutAppendReply)
	reply.Err = putAppendReply.Err
	return
}

func (kv *ShardKV) MoveShard(args *MoveShardArgs, reply *MoveShardReply) {
	moveArgs := MoveShardArgs{
		ConfigNum:  args.ConfigNum,
		Shard2Data: args.Shard2Data}
	op := Op{
		Type:     MOVE,
		UUID:     args.UUID,
		MoveArgs: moveArgs,
	}
	ok, err, opReply := kv.SendOpToRaft(op)
	if ok == false {
		reply.Err = err
		return
	}
	moveShardReply, ok := opReply.Reply.(MoveShardReply)
	reply.Err = moveShardReply.Err
	return
}
func (kv *ShardKV) HandleMoveSucc(args *MoveSuccArgs, reply *MoveSuccReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for idx := range args.ShardsIdx {
		if kv.managedShards[idx].State == LEAVING {
			kv.managedShards[idx] = Exists{State: LEFT}
		}
	}
	reply.Err = OK
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	fmt.Printf("start server\n")
	labgob.Register(Op{})
	labgob.Register(MoveShardArgs{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.msgIndex = 0
	kv.opReply = make(map[int]OpReply, 1000)
	kv.uuidSet = make(map[string]int, 1000)
	kv.managedShards = make(map[int]Exists, 10)
	kv.kvMap = make(map[string]string, 1000)
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.detectionConfigChange()
	go kv.operationProcess()
	return kv
}

func (kv *ShardKV) detectionConfigChange() {
	for {
		time.Sleep(TimeSleepForGetLatestConfig)
		kv.mu.Lock()
		oldConfig := kv.latestConfig
		kv.mu.Unlock()
		var newConfig shardctrler.Config
		queryConfig := kv.mck.Query(oldConfig.Num + 1)
		copyConfig(&queryConfig, &newConfig)
		kv.mu.Lock()
		DPrintf("gid %d me %d detect oldNum is %d newNum is %d managedShard is %v", kv.gid, kv.me, oldConfig.Num, newConfig.Num, kv.managedShards)
		DPrintf("gid %d me %d opres %v, kvmap %v", kv.gid, kv.me, kv.opReply, kv.kvMap)
		kv.mu.Unlock()
		if oldConfig.Num != newConfig.Num {
			// 如果Num不一致，则说明config发生了变化，因此要对shard进行迁移，这里迁移是通过RPC实现的。
			kv.handleConfigChange(oldConfig, newConfig)
		}
	}
}

func (kv *ShardKV) handleConfigChange(oldConfig shardctrler.Config, newConfig shardctrler.Config) {
	oldManagedShards := getShardsManagedByTheGid(oldConfig, kv.gid)
	newManagedShards := getShardsManagedByTheGid(newConfig, kv.gid)
	leftShards := make([]int, 0, shardctrler.NShards)
	kv.mu.Lock()
	kv.latestConfig = newConfig
	for shardId := range oldManagedShards {
		if kv.managedShards[shardId].State == COMING ||
			kv.managedShards[shardId].State == LEFT {
			continue
		}
		if _, ok := newManagedShards[shardId]; ok == false {
			leftShards = append(leftShards, shardId)
			kv.managedShards[shardId] = Exists{State: LEAVING}
		}
	}
	for shardId := range newManagedShards {
		if _, ok := oldManagedShards[shardId]; ok == false {
			if oldConfig.Shards[shardId] == 0 {
				kv.managedShards[shardId] = Exists{State: EXIST}
			} else {
				kv.managedShards[shardId] = Exists{State: COMING}
			}
		} else {
			if kv.managedShards[shardId].State == LEFT {
				kv.managedShards[shardId] = Exists{State: COMING}
			} else if kv.managedShards[shardId].State == LEAVING {
				kv.managedShards[shardId] = Exists{State: EXIST}
			}
		}
	}
	DPrintf("gid:%v me:%v managedShards%v leftShards %v\n", kv.gid, kv.me, kv.managedShards, leftShards)
	kv.mu.Unlock()
	kv.moveShardsToNewGids(leftShards, newConfig)
}

func (kv *ShardKV) moveShardsToNewGids(shards []int, newConfig shardctrler.Config) {
	kv.mu.Lock()
	kvMap := kv.kvMap
	leaveMsgs := getGidMsgForMove(shards, kvMap, newConfig)
	DPrintf("leaveMsgs %#v", leaveMsgs)
	kv.mu.Unlock()
	wg := sync.WaitGroup{}
	for gid := range leaveMsgs {
		DPrintf("moveShardsTonewGids gid is %v\n", gid)
		go func(gid int) {
			wg.Add(1)
			defer wg.Done()
			args := MoveShardArgs{
				Shard2Data: leaveMsgs[gid].Shard2Data,
				ConfigNum:  newConfig.Num,
				UUID:       generateUUID()}
			for {
				kv.mu.Lock()
				leader := kv.leader
				kv.mu.Unlock()
				if leader == false {
					break
				}
				if servers, ok := newConfig.Groups[gid]; ok {
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						var reply MoveShardReply
						ok := srv.Call("ShardKV.MoveShard", &args, &reply)
						DPrintf("gid %v me %vmove reply %v", kv.gid, kv.me, reply)
						if ok && reply.Err == OK {
							kv.mu.Lock()
							for shard := range leaveMsgs[gid].Shard2Data {
								kv.managedShards[shard] = Exists{State: LEFT}
							}
							kv.mu.Unlock()
							// leader向同组的其他server发送成功的消息
							shardIds := getShardsIdxFromShard2Data(args.Shard2Data)
							go kv.sendMoveSuccToServerInSameGroup(shardIds, newConfig)
							return
						}
						if reply.Err == ErrConfigOutDated {
							return
						}
					}
				}
			}
		}(gid)
	}
	wg.Wait()
}

func (kv *ShardKV) sendMoveSuccToServerInSameGroup(shardsIdx []int, newConfig shardctrler.Config) {
	args := MoveSuccArgs{ShardsIdx: shardsIdx}
	if servers, ok := newConfig.Groups[kv.gid]; ok {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply MoveSuccReply
			ok := false
			for ok == false {
				ok = srv.Call("ShardKV.HandleMoveSucc", &args, &reply)
			}
		}
	}
}

func (kv *ShardKV) operationProcess() {
	fmt.Printf("start operationProcess\n")
	for msg := range kv.applyCh {
		if kv.gid == 101 {
			fmt.Printf("msg is %v\n", msg)
		}
		//kv.mu.Lock()
		//latestIndex := kv.msgIndex
		//kv.mu.Unlock()
		//if msg.CommandIndex <= latestIndex || msg.CommandValid == false {
		//	continue
		//}
		op, ok := msg.Command.(Op)
		if ok == false {
			break
		}
		kv.mu.Lock()
		idx, ok := kv.uuidSet[op.UUID]
		kv.mu.Unlock()
		if ok == true {
			kv.mu.Lock()
			kv.opReply[msg.CommandIndex] = kv.opReply[idx]
			kv.mu.Unlock()
			continue
		}
		var opReply OpReply
		var reply interface{}
		switch op.Type {
		case GET:
			reply = kv.getProcessing(op.Key)
		case PUT:
			reply = kv.putProcessing(op.Key, op.Value)
		case APPEND:
			reply = kv.appendProcessing(op.Key, op.Value)
		case MOVE:
			reply = kv.moveShardProcessing(op.MoveArgs)
		}
		opReply = OpReply{
			Type:  op.Type,
			Reply: reply,
			UUID:  op.UUID,
		}
		kv.mu.Lock()
		kv.opReply[msg.CommandIndex] = opReply
		kv.uuidSet[op.UUID] = msg.CommandIndex
		kv.msgIndex = msg.CommandIndex
		kv.mu.Unlock()
	}
}

// 实际上在处理get、put和append的时候都要判断当前的kv服务器是否是储存该分片的服务器。
// 如果不是，应该返回一个error

func (kv *ShardKV) getProcessing(key string) interface{} {
	reply := GetReply{}
	kv.mu.Lock()
	value, ok := kv.kvMap[key]
	kv.mu.Unlock()
	if ok == false {
		reply.Err = ErrNoKey
	} else {
		reply.Value = value
		reply.Err = OK
	}
	return reply
}

func (kv *ShardKV) putProcessing(key string, value string) interface{} {
	kv.mu.Lock()
	kv.kvMap[key] = value
	kv.mu.Unlock()
	return PutAppendReply{Err: OK}
}

func (kv *ShardKV) appendProcessing(key string, value string) interface{} {
	kv.mu.Lock()
	kv.kvMap[key] += value
	kv.mu.Unlock()
	return PutAppendReply{Err: OK}
}

func (kv *ShardKV) moveShardProcessing(args interface{}) interface{} {
	moveShardArgs, ok := args.(MoveShardArgs)
	if ok == false {
		return MoveShardReply{Err: ErrChangeType}
	}
	kv.mu.Lock()
	for kv.latestConfig.Num < moveShardArgs.ConfigNum {
		kv.mu.Unlock()
		time.Sleep(TimeSleepForGetLatestConfig)
		kv.mu.Lock()
	}
	kv.mu.Unlock()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//if moveShardArgs.ConfigNum != kv.latestConfig.Num {
	//	return MoveShardReply{Err: ErrConfigOutDated}
	//}
	// 这里改为检测发送过来的args是否是这个shard管理的args
	for shard, data := range moveShardArgs.Shard2Data {
		if kv.managedShards[shard].State != COMING {
			continue
		}
		for key, value := range data {
			kv.kvMap[key] = value
		}
		kv.managedShards[shard] = Exists{State: EXIST}
	}
	return MoveShardReply{Err: OK}

}

// 还有一个要注意的问题，就是说在什么时候发送RPC迁移shared，还有就是在迁移之后是否要删除原来database中的数据
