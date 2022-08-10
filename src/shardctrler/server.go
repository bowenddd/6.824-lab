package shardctrler

import (
	"6.824/raft"
	"sort"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	// Your data here.
	cmdIdx    int      // max raft agreement index
	configs   []Config // indexed by config num
	msgResult map[int]MsgReply
	uuidSet   map[string]int
	gidShards map[int][]int
	//latestNum int                // latest config num
	leader bool
}

type Gid2Shard struct {
	Gid   int
	Shard []int
	Num   int
}

type GidRedisMsg struct {
	ShuldNum int
	HaveNum  int
}

type Op struct {
	// Your data here.
	Type string
	Args interface{}
	UUID string
}

type MsgReply struct {
	Type  string
	Reply interface{}
	UUID  string
}

// 判断某条指令是否在3s内在raft server中达成共识
func (sc *ShardCtrler) Agreement(index int, cmd Op) bool {
	sc.mu.Lock()
	res, ok := sc.msgResult[index]
	sc.mu.Unlock()
	now := time.Now()
	for ok == false && time.Since(now) < 3*time.Second {
		time.Sleep(10 * time.Millisecond)
		sc.mu.Lock()
		res, ok = sc.msgResult[index]
		sc.mu.Unlock()
	}
	if ok == false {
		return false
	}
	if cmd.Type != res.Type || cmd.UUID != res.UUID {
		return false
	}
	return true
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	cmd := Op{
		Type: JOIN,
		Args: JoinArgs{Servers: args.Servers},
		UUID: args.UUID,
	}
	// 将客服端发送的join操作发送到raft中达成一致性共识
	// 如果没有达成共识，返回false，以及相应的error
	index, _, leader := sc.rf.Start(cmd)
	sc.mu.Lock()
	sc.leader = leader
	sc.mu.Unlock()
	if leader == false {
		reply.WrongLeader = true
		reply.Err = WrongLeader
		return
	}
	reply.WrongLeader = false
	if sc.Agreement(index, cmd) == false {
		reply.Err = NotAgreement
		return
	}
	// 达成共识开始具体的业务逻辑，下面开始具体的业务逻辑
	// join就是把GID让2f+1个raft server进行复制容错
	// 这里是否可以用go命令呢？不可以，而且不能把业务逻辑写在这个地方，
	// 因为这样的话可能不满足线性化。
	sc.mu.Lock()
	res := sc.msgResult[index]
	sc.mu.Unlock()
	joinReply, ok := res.Reply.(JoinReply)
	if ok == false {
		reply.Err = WrongType
		return
	}
	reply.Err = joinReply.Err
	return
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	cmd := Op{
		Type: LEAVE,
		Args: LeaveArgs{GIDs: args.GIDs},
		UUID: args.UUID,
	}
	// 将客服端发送的join操作发送到raft中达成一致性共识
	// 如果没有达成共识，返回false，以及相应的error
	index, _, leader := sc.rf.Start(cmd)
	sc.mu.Lock()
	sc.leader = leader
	sc.mu.Unlock()
	if leader == false {
		reply.WrongLeader = true
		reply.Err = WrongLeader
		return
	}
	reply.WrongLeader = false
	if sc.Agreement(index, cmd) == false {
		reply.Err = NotAgreement
		return
	}
	// 达成共识开始具体的业务逻辑，下面开始具体的业务逻辑
	// join就是把GID让2f+1个raft server进行复制容错
	// 这里是否可以用go命令呢？不可以，而且不能把业务逻辑写在这个地方，
	// 因为这样的话可能不满足线性化。
	sc.mu.Lock()
	res := sc.msgResult[index]
	sc.mu.Unlock()
	leaveReply, ok := res.Reply.(LeaveReply)
	if ok == false {
		reply.Err = WrongType
		return
	}
	reply.Err = leaveReply.Err
	return
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	cmd := Op{
		Type: MOVE,
		Args: MoveArgs{GID: args.GID, Shard: args.Shard},
		UUID: args.UUID,
	}
	// 将客服端发送的join操作发送到raft中达成一致性共识
	// 如果没有达成共识，返回false，以及相应的error
	index, _, leader := sc.rf.Start(cmd)
	sc.mu.Lock()
	sc.leader = leader
	sc.mu.Unlock()
	if leader == false {
		reply.WrongLeader = true
		reply.Err = WrongLeader
		return
	}
	reply.WrongLeader = false
	if sc.Agreement(index, cmd) == false {
		reply.Err = NotAgreement
		return
	}
	// 达成共识开始具体的业务逻辑，下面开始具体的业务逻辑
	// join就是把GID让2f+1个raft server进行复制容错
	// 这里是否可以用go命令呢？不可以，而且不能把业务逻辑写在这个地方，
	// 因为这样的话可能不满足线性化。
	sc.mu.Lock()
	res := sc.msgResult[index]
	sc.mu.Unlock()
	moveReply, ok := res.Reply.(MoveReply)
	if ok == false {
		reply.Err = WrongType
		return
	}
	reply.Err = moveReply.Err
	return
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	cmd := Op{
		Type: QUERY,
		Args: QueryArgs{Num: args.Num},
		UUID: args.UUID,
	}
	// 将客服端发送的join操作发送到raft中达成一致性共识
	// 如果没有达成共识，返回false，以及相应的error
	index, _, leader := sc.rf.Start(cmd)
	sc.mu.Lock()
	sc.leader = leader
	sc.mu.Unlock()
	if leader == false {
		reply.WrongLeader = true
		reply.Err = WrongLeader
		return
	}
	reply.WrongLeader = false
	if sc.Agreement(index, cmd) == false {
		reply.Err = NotAgreement
		return
	}
	// 达成共识开始具体的业务逻辑，下面开始具体的业务逻辑
	// join就是把GID让2f+1个raft server进行复制容错
	// 这里是否可以用go命令呢？不可以，而且不能把业务逻辑写在这个地方，
	// 因为这样的话可能不满足线性化。
	sc.mu.Lock()
	res := sc.msgResult[index]
	sc.mu.Unlock()
	queryReply, ok := res.Reply.(QueryReply)
	if ok == false {
		reply.Err = WrongType
		return
	}
	reply.Err = queryReply.Err
	reply.Config = queryReply.Config
	DPrintf("query result is %#v\n", reply.Config)
	sc.mu.Lock()
	for _, config := range sc.configs {
		DPrintf("config query %#v\n", config)
	}
	sc.mu.Unlock()
	return
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(QueryArgs{})
	labgob.Register(MoveArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.cmdIdx = 0
	sc.msgResult = make(map[int]MsgReply, 500)
	sc.uuidSet = make(map[string]int, 500)
	sc.gidShards = make(map[int][]int, 10)
	sc.leader = false
	go sc.configurationProcess()
	return sc
}

func (sc *ShardCtrler) configurationProcess() {
	for msg := range sc.applyCh {
		sc.mu.Lock()
		latestCmdIdx := sc.cmdIdx
		// latestCondigNum := sc.latestNum
		sc.mu.Unlock()
		if msg.CommandIndex < latestCmdIdx || msg.CommandValid == false {
			continue
		}
		op, ok := msg.Command.(Op)
		if ok == false {
			DPrintf("receive apply msg error, the msg is %#v\n", msg)
			break
		}
		sc.mu.Lock()
		idx, ok := sc.uuidSet[op.UUID]
		sc.mu.Unlock()
		if ok == true && op.Type != QUERY {
			sc.mu.Lock()
			sc.msgResult[msg.CommandIndex] = sc.msgResult[idx]
			sc.mu.Unlock()
			continue
		}
		var msgReply MsgReply
		var reply interface{}
		switch op.Type {
		case JOIN:
			reply = sc.joinProcessing(op)
		case LEAVE:
			reply = sc.leaveProcessing(op)
		case MOVE:
			reply = sc.moveProcessing(op)
		case QUERY:
			reply = sc.queryProcessing(op)
		}
		msgReply = MsgReply{
			Type:  op.Type,
			UUID:  op.UUID,
			Reply: reply}
		sc.mu.Lock()
		sc.msgResult[msg.CommandIndex] = msgReply
		sc.uuidSet[op.UUID] = msg.CommandIndex
		sc.cmdIdx = msg.CommandIndex
		sc.mu.Unlock()
	}
}

// 下面是具体的业务逻辑

func copyGroups(src map[int][]string, dst map[int][]string) {
	for k, v := range src {
		dst[k] = v
	}
	return
}

func copyShards(src *[NShards]int, dst *[NShards]int) {
	for i, gid := range src {
		dst[i] = gid
	}
	return
}

func copyGidShards(src map[int][]int, dst map[int][]int) {
	for k, v := range src {
		dst[k] = v
	}
	return
}

func containsGid(gids []int, gid int) bool {
	for _, v := range gids {
		if gid == v {
			return true
		}
	}
	return false
}

func getGIDs(group map[int][]string) []int {
	gids := make([]int, 0, len(group))
	for key := range group {
		gids = append(gids, key)
	}
	return gids
}

// 将新加入的group与之前configuration中的group合并，
// 如果重复以最新的信息为准。
func mergeGroups(last map[int][]string, new map[int][]string) map[int][]string {
	group := make(map[int][]string, len(last)+len(new))
	for k, v := range last {
		group[k] = v
	}
	for k, v := range new {
		group[k] = v
	}
	return group
}

// 向上取整的函数
func ceil(a int, b int) int {
	return (a + b - 1) / b
}

// 这个方法是将gid和shard的映射关系转化为列表，用于排序
func gidShardSlience(gidShard map[int][]int) []Gid2Shard {
	list := make([]Gid2Shard, 0, len(gidShard))
	for k, v := range gidShard {
		list = append(list, Gid2Shard{Gid: k, Shard: v, Num: len(v)})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].Num > list[j].Num {
			return true
		}
		if list[i].Num < list[j].Num {
			return false
		}
		return list[i].Gid < list[j].Gid
	})
	return list
}

func judgeInitStatus(gidShards map[int][]int) bool {
	sum := 0
	for _, v := range gidShards {
		sum += len(v)
	}
	if sum == 0 {
		return true
	}
	return false
}

func handleForRedistribute(gidShards map[int][]int, leftShardNum int, leftGidNum int) (
	[]int, map[int]int) {
	gid2ShardList := gidShardSlience(gidShards)
	shiftShardList := make([]int, 0, NShards)
	if judgeInitStatus(gidShards) {
		for i := 0; i < NShards; i++ {
			shiftShardList = append(shiftShardList, i)
		}
	}
	gidShardMsg := make(map[int]int, 10)
	for _, gid2shard := range gid2ShardList {
		retainNum := ceil(leftShardNum, leftGidNum)
		shiftNum := gid2shard.Num - retainNum
		if shiftNum < 0 {
			gidShardMsg[gid2shard.Gid] = retainNum - gid2shard.Num
		}
		if shiftNum > 0 {
			shiftShardList = append(shiftShardList, gid2shard.Shard[:shiftNum]...)
			gid2shard.Shard = gid2shard.Shard[shiftNum:]
			gid2shard.Num -= shiftNum
		}
		//rand.Seed(42)
		//for shiftNum > 0 {
		//	removeIdx := rand.Intn(gid2shard.Num)
		//	shiftShardList = append(shiftShardList, gid2shard.Shard[removeIdx])
		//	gid2shard.Shard = append(gid2shard.Shard[:removeIdx], gid2shard.Shard[removeIdx+1:]...)
		//	gid2shard.Num -= 1
		//	shiftNum -= 1
		//}
		leftShardNum -= retainNum
		leftGidNum -= 1
	}
	return shiftShardList, gidShardMsg
}

// 重新对shard分区做负载均衡(此方法用于join操作的负载均衡)
func (sc *ShardCtrler) shardRedistributeForJoin(shards *[NShards]int, newGids []int, preNum int, newNum int) {
	if sc.leader {
		DPrintf("shards %v newGids %v preNum %v newNum %v gidShards%v\n", shards, newGids, preNum, newNum, sc.gidShards)
	}
	if preNum == newNum {
		return
	}
	if preNum < newNum {
		gidShards := make(map[int][]int, NShards)
		copyGidShards(sc.gidShards, gidShards)
		for _, gid := range newGids {
			if _, ok := gidShards[gid]; ok == false {
				gidShards[gid] = []int{}
			}
		}
		shiftShardList, gidShardMsg := handleForRedistribute(gidShards, NShards, newNum)
		// 上面的代码已经把多的shard拿出来了，下面就是将这些多的shard放到新加入的gid组中，ok先去核酸核酸。
		shardRedistribute(shards, shiftShardList, gidShardMsg)
	} else {
		DPrintf("redistribute error! because preLen:%d > curLen %d\n", preNum, newNum)
	}
}

func getGidShardMsgKeys(gidShardMsg map[int]int) []int {
	gidShardMsgSlience := make([]Gid2Shard, 0, len(gidShardMsg))
	for k, v := range gidShardMsg {
		gidShardMsgSlience = append(gidShardMsgSlience, Gid2Shard{Gid: k, Num: v})
	}
	sort.Slice(gidShardMsgSlience, func(i, j int) bool {
		return gidShardMsgSlience[i].Num < gidShardMsgSlience[j].Num
	})
	keys := make([]int, 0, len(gidShardMsg))
	for _, gid2shard := range gidShardMsgSlience {
		keys = append(keys, gid2shard.Gid)
	}
	sort.Ints(keys)
	return keys
}

func shardRedistribute(shards *[NShards]int, shiftShardList []int, gidShardMsg map[int]int) {
	idx := 0
	gids := getGidShardMsgKeys(gidShardMsg)
	for _, gid := range gids {
		num := gidShardMsg[gid]
		for i := idx; i < idx+num; i++ {
			shards[shiftShardList[i]] = gid
		}
		idx += num
	}
}

func (sc *ShardCtrler) shardGidUpdate(shard [NShards]int) {
	gidShards := make(map[int][]int, 10)
	for i, gid := range shard {
		gidShards[gid] = append(gidShards[gid], i)
	}
	if _, ok := gidShards[0]; ok == true {
		delete(gidShards, 0)
	}
	sc.gidShards = gidShards
}

func (sc *ShardCtrler) joinProcessing(op Op) interface{} {
	args, ok := op.Args.(JoinArgs)
	reply := JoinReply{}
	if ok == false {
		reply.Err = WrongType
		DPrintf("change args to joinArgs error, the msg is %#v\n", op)
		return reply
	}
	sc.mu.Lock()
	prevConfig := sc.configs[len(sc.configs)-1]
	if sc.leader {
		DPrintf("prevConfig %#v op %#v\n", prevConfig, op)
	}
	newGids := make([]int, 0, len(args.Servers))
	for k := range args.Servers {
		if _, ok := prevConfig.Groups[k]; ok == false {
			newGids = append(newGids, k)
		}
	}
	newGroups := mergeGroups(prevConfig.Groups, args.Servers)
	shards := [NShards]int{}
	copyShards(&prevConfig.Shards, &shards)
	sort.Ints(newGids)
	sc.shardRedistributeForJoin(&shards, newGids, len(prevConfig.Groups), len(newGroups))
	newConfig := Config{
		Num:    prevConfig.Num + 1,
		Shards: shards,
		Groups: newGroups,
	}
	sc.configs = append(sc.configs, newConfig)
	//别忘了还要更新sc.gidShards
	sc.shardGidUpdate(shards)
	sc.mu.Unlock()
	reply.Err = OK
	return reply
}

func removeGidsFromGroups(groups map[int][]string, gids []int) {
	for _, gid := range gids {
		delete(groups, gid)
	}
}

func (sc *ShardCtrler) shardRedistributeForLeave(shards *[NShards]int, removeGids []int, newGids []int) {
	// 首先要把在删除的gid中管理的shard都拿出来，将他们均匀地放到剩下的gid中
	if len(newGids) == 0 {
		for i, _ := range shards {
			shards[i] = 0
		}
		return
	}
	shiftShardList := make([]int, 0, NShards)
	gidShards := make(map[int][]int, NShards)
	copyGidShards(sc.gidShards, gidShards)
	for _, gid := range removeGids {
		shiftShardList = append(shiftShardList, sc.gidShards[gid]...)
		delete(gidShards, gid)
	}
	for _, gid := range newGids {
		if _, ok := gidShards[gid]; ok == false {
			gidShards[gid] = []int{}
		}
	}
	// 这里拿到来要转移的shard列表，看看能不能把转移的逻辑抽象出来
	shiftList, gidShardMsg := handleForRedistribute(gidShards, NShards, len(newGids))
	if sc.leader {
		DPrintf("shiftList %v gidshardNum %d\n", shiftList, gidShardMsg)
	}
	shiftShardList = append(shiftShardList, shiftList...)
	if sc.leader {
		DPrintf("shiftShardList %v gidshardMsg %d\n", shiftShardList, gidShardMsg)
	}
	shardRedistribute(shards, shiftShardList, gidShardMsg)

}
func (sc *ShardCtrler) leaveProcessing(op Op) interface{} {
	args, ok := op.Args.(LeaveArgs)
	reply := LeaveReply{}
	if ok == false {
		reply.Err = WrongType
		DPrintf("change args to leaveArgs error, the msg is %#v\n", op)
		return reply
	}
	sc.mu.Lock()
	prevConfig := sc.configs[len(sc.configs)-1]
	if sc.leader {
		DPrintf("prevConfig %#v op %#v\n", prevConfig, op)
	}
	newGroups := make(map[int][]string, len(prevConfig.Groups))
	copyGroups(prevConfig.Groups, newGroups)
	removeGidsFromGroups(newGroups, args.GIDs)
	newGids := getGIDs(newGroups)
	if sc.leader {
		DPrintf("leave newGids %d\n", len(newGids))
	}
	shards := [NShards]int{}
	copyShards(&prevConfig.Shards, &shards)
	sc.shardRedistributeForLeave(&shards, args.GIDs, newGids)
	newConfig := Config{
		Num:    prevConfig.Num + 1,
		Shards: shards,
		Groups: newGroups,
	}
	sc.configs = append(sc.configs, newConfig)
	//别忘了还要更新sc.gidShards
	sc.shardGidUpdate(shards)
	sc.mu.Unlock()
	reply.Err = OK
	return reply

}

func (sc *ShardCtrler) moveProcessing(op Op) interface{} {
	// 这里先想一下move需要进行的操作
	// 参数是一个分片号，和一个GID，
	args, ok := op.Args.(MoveArgs)
	reply := MoveReply{}
	if ok == false {
		reply.Err = WrongType
		DPrintf("change args to moveArgs error, the msg is %#v\n", op)
		return reply
	}
	sc.mu.Lock()
	prevConfig := sc.configs[len(sc.configs)-1]
	gids := getGIDs(prevConfig.Groups)
	if !containsGid(gids, args.GID) {
		reply.Err = InvalidArgs
		return reply
	}
	shards := [NShards]int{}
	newGroups := make(map[int][]string, len(prevConfig.Groups))
	copyGroups(prevConfig.Groups, newGroups)
	copyShards(&prevConfig.Shards, &shards)
	// 执行move操作
	shards[args.Shard] = args.GID
	newConfig := Config{
		Num:    prevConfig.Num + 1,
		Shards: shards,
		Groups: newGroups,
	}
	sc.configs = append(sc.configs, newConfig)
	//别忘了还要更新sc.gidShards
	sc.shardGidUpdate(shards)
	sc.mu.Unlock()
	reply.Err = OK
	return reply
}

func (sc *ShardCtrler) queryProcessing(op Op) interface{} {
	args, ok := op.Args.(QueryArgs)
	reply := QueryReply{}
	if ok == false {
		reply.Err = WrongType
		DPrintf("change args to queryArgs error, the msg is %#v\n", op)
		return reply
	}
	if args.Num < -1 {
		reply.Err = InvalidArgs
		return reply
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	latestConfig := sc.configs[len(sc.configs)-1]
	if args.Num == -1 || args.Num >= latestConfig.Num {
		reply.Config = latestConfig
		reply.Err = OK
		return reply
	}
	for _, config := range sc.configs {
		if config.Num == args.Num {
			reply.Config = config
			reply.Err = OK
			return reply
		}
	}
	reply.Err = InvalidArgs
	return reply
}

// 这里说一下现在的理解
// 在shardcteler的配置中有许多复制组，他们有唯一的GID。
// 每一个复制组由一些raft server来实现分布式一致性。
// 所以有一个map实现从gid到raft server的映射。
// 上面的这个东西现在和shard分片没有关系。
// 对于分片而言，实际上就是哈希，我们的目标是把这些分片尽可能
// 平均的放到已有的复制组中，并且移动尽可能少的分片
// e.g. 比如我们有一个复制组，那么我们只能把所有的分片放到这一个复制组中
// 而如果我们有五个复制组，那么我们要对分片进行重新分配把他们尽可能平均放到这五个复制组中。
// redistribute
