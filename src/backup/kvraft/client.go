package kvraft

import (
	"6.824/labrpc"
	"fmt"
	uuid2 "github.com/satori/go.uuid"
	"math"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderServer int
	mu           sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderServer = math.MinInt32
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func generateUUID(str string) string {
	//return fmt.Sprintf("%s", uuid2.NewV4())
	return fmt.Sprintf("%s-%s-%d", uuid2.NewV4(), uuid2.NewV5(uuid2.NamespaceURL, str), time.Now().UnixNano())
}

func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	DPrintf("use get key is %s\n", key)
	var ok bool
	var value string
	succ := false
	getArgs := GetArgs{Key: key, UUID: generateUUID(fmt.Sprintf("%s-%s", key, value))}
	for succ == false {
		ck.mu.Lock()
		leader := ck.leaderServer
		ck.mu.Unlock()
		for leader != math.MinInt32 && succ == false {
			ok = false
			getReply := GetReply{}
			ok = ck.servers[leader].Call("KVServer.Get", &getArgs, &getReply)
			if ok == false {
				break
			}
			switch getReply.Err {
			case OK, ErrNoKey:
				succ = true
				value = getReply.Value
			case ErrWrongLeader:
				ck.mu.Lock()
				ck.leaderServer = math.MinInt32
				leader = ck.leaderServer
				ck.mu.Unlock()
			default:
			}
		}
		if succ == true {
			break
		}
		// 向所有的server发送rpc
		var wg sync.WaitGroup
		for i := 0; i < len(ck.servers); i++ {
			wg.Add(1)
			ck.mu.Lock()
			go func(x int) {
				ck.mu.Unlock()
				defer wg.Done()
				reply := GetReply{}
				okk := false
				okk = ck.servers[x].Call("KVServer.Get", &getArgs, &reply)
				if okk == false {
					return
				}
				switch reply.Err {
				case OK, ErrNoKey:
					succ = true
					value = reply.Value
					ck.mu.Lock()
					ck.leaderServer = x
					ck.mu.Unlock()
				case ErrNoAgreement:
					ck.mu.Lock()
					ck.leaderServer = x
					ck.mu.Unlock()
				default:
				}
			}(i)
		}
		wg.Wait()
	}
	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf("use put append, key %s value %s op %s\n", key, value, op)
	var ok bool
	succ := false
	uuid := generateUUID(fmt.Sprintf("%s-%s-%s", key, value, op))
	putAppendArgs := PutAppendArgs{Key: key, Value: value, Op: op, UUID: uuid}
	for succ == false {
		ck.mu.Lock()
		leader := ck.leaderServer
		ck.mu.Unlock()
		for leader != math.MinInt32 && succ == false {
			ok = false
			putAppendReply := PutAppendReply{}
			ok = ck.servers[leader].Call("KVServer.PutAppend", &putAppendArgs, &putAppendReply)
			if ok == false {
				DPrintf("key %s value %s op %s sengRPC to %d failure\n", key, value, op, leader)
				break
			}
			DPrintf("key %s value %s op %s reply %v\n", key, value, op, putAppendReply)
			switch putAppendReply.Err {
			case OK, ErrNoKey:
				succ = true
			case ErrWrongLeader:
				ck.mu.Lock()
				ck.leaderServer = math.MinInt32
				leader = ck.leaderServer
				ck.mu.Unlock()
			default:
			}
		}
		if succ == true {
			break
		}
		// 向所有的server发送rpc
		var wg sync.WaitGroup
		for i := 0; i < len(ck.servers); i++ {
			wg.Add(1)
			ck.mu.Lock()
			go func(x int) {
				ck.mu.Unlock()
				defer wg.Done()
				reply := PutAppendReply{}
				okk := false
				okk = ck.servers[x].Call("KVServer.PutAppend", &putAppendArgs, &reply)
				if okk == false {
					DPrintf("server key %s value %s op %s sengRPC to %d failure\n", key, value, op, x)
					return
				}
				DPrintf("server %d key %s value %s op %s reply %v\n", x, key, value, op, reply)
				switch reply.Err {
				case OK, ErrNoKey:
					succ = true
					ck.mu.Lock()
					ck.leaderServer = x
					ck.mu.Unlock()
				case ErrNoAgreement:
					ck.mu.Lock()
					ck.leaderServer = x
					ck.mu.Unlock()
				default:
				}
			}(i)
		}
		wg.Wait()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// 先写一下思路，在这里客户端要向所有的server发送RPC消息，但是只有与raft的leader相连的server才可以
// 处理客户端发送的请求，客户端通过判断返回的error信息，只要不是ErrWrongLeader就储存leader的编号，
// 只需要向
