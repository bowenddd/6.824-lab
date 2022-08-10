package shardkv

import (
	"6.824/shardctrler"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"log"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrNoAgreement    = "ErrNoAgreement"
	ErrChangeType     = "ErrChangeType"
	ErrConfigOutDated = "ErrConfigOutDated"
)

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
	MOVE   = "Move"
)

const (
	AgreementMaxTime            = 3 * time.Second
	AgreementWaitTime           = 10 * time.Millisecond
	TimeSleepForGetLatestConfig = 100 * time.Millisecond
)

// gid管理的shard的状态
const (
	EXIST   = "Exist"
	LEFT    = "Left"
	LEAVING = "Leaving"
	COMING  = "Coming"
)

const Debug = true

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	UUID string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	UUID string
}

type GetReply struct {
	Err   Err
	Value string
}

// 发送shard的rpc的args
type MoveShardArgs struct {
	Shard2Data map[int]map[string]string
	ConfigNum  int
	UUID       string
}

type MoveShardReply struct {
	Err Err
}

type LeaveMsgForGid struct {
	Shard2Data map[int]map[string]string
}

type MoveSuccArgs struct {
	ShardsIdx []int
}

type MoveSuccReply struct {
	Err Err
}

func generateUUID() string {
	return fmt.Sprintf("%s-%d", uuid.NewV4(), time.Now().UnixNano())
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func copyConfig(src *shardctrler.Config, dst *shardctrler.Config) {
	dst.Num = src.Num
	dst.Shards = src.Shards
	dst.Groups = src.Groups
}

func getShardsManagedByTheGid(config shardctrler.Config, gid int) map[int]Exists {
	shards := make(map[int]Exists, shardctrler.NShards)
	for idx, id := range config.Shards {
		if id == gid {
			shards[idx] = Exists{}
		}
	}
	return shards
}

// 说一下这里的分片策略，就是按key的首字母的ascii码来进行分片
// 让首字母的ascii码来 % shard的数量就是key的分片
// 然后每个分片再找一些raft组来完成一致性复制
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func detectCorrectServer(key string, managedShards map[int]Exists) bool {
	shard := key2shard(key)
	exists, ok := managedShards[shard]
	return ok && exists.State == EXIST
}

func searchDataByShardId(data map[string]string, shardId int) map[string]string {
	searchData := make(map[string]string)
	for key, value := range data {
		shard := key2shard(key)
		if shard == shardId {
			searchData[key] = value
		}
	}
	return searchData
}

func getGidMsgForMove(leaveShards []int, data map[string]string, config shardctrler.Config) map[int]LeaveMsgForGid {
	moveMsg := map[int]LeaveMsgForGid{}
	for key := range config.Groups {
		shard2Data := make(map[int]map[string]string)
		for _, shard := range leaveShards {
			if config.Shards[shard] == key {
				shard2Data[shard] = searchDataByShardId(data, shard)
			}
		}
		if len(shard2Data) > 0 {
			moveMsg[key] = LeaveMsgForGid{Shard2Data: shard2Data}
		}
	}
	return moveMsg
}

func mergeMoveData(datas []map[string]string) map[string]string {
	moveData := make(map[string]string)
	for _, data := range datas {
		for k, v := range data {
			moveData[k] = v
		}
	}
	return moveData
}

func getShardsIdxFromShard2Data(shard2Data map[int]map[string]string) []int {
	shardsIdx := make([]int, 0, len(shard2Data))
	for key := range shard2Data {
		shardsIdx = append(shardsIdx, key)
	}
	return shardsIdx
}
