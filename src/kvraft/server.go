package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	HandleOpTimeOut = time.Millisecond * 2000
)

type OType int

const (
	OPGet OType = iota
	OPPut
	OPAppend
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType     OType
	Key        string
	Val        string
	Seq        uint64
	Identifier int64
}

type result struct {
	LastSeq uint64
	Err     Err
	Value   string
	ResTerm int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	waiCh      map[int]*chan result
	historyMap map[int64]*result

	maxraftstate int // snapshot if log grows this big
	maxMapLen    int
	db           map[string]string
	persister    *raft.Persister
	lastApplied  int

	// Your definitions here.
}

func (kv *KVServer) HandleOp(opArgs *Op) (res result) {
	kv.mu.Lock()
	if hasMap, exist := kv.historyMap[opArgs.Identifier]; exist {
		if hasMap.LastSeq == opArgs.Seq {
			kv.mu.Unlock()
			return *hasMap
		}
	}
	kv.mu.Unlock()

	startIndex, startTerm, isLeader := kv.rf.Start(*opArgs)
	//fmt.Printf("HandleOp: OP: %v\n", *opArgs)
	if !isLeader {
		return result{Err: ErrWrongLeader, Value: ""}
	}

	kv.mu.Lock()
	// 协程处理完毕
	newCh := make(chan result)
	kv.waiCh[startIndex] = &newCh
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.waiCh, startIndex)
		close(newCh)
		kv.mu.Unlock()
	}()

	select {
	case <-time.After(HandleOpTimeOut):
		res.Err = ErrHandleOpTimeOut
		return
	case msg, success := <-newCh:
		if success && msg.ResTerm == startTerm {
			res = msg
			return
		} else if !success {
			res.Err = ErrChanClose
			return
		} else {
			res.Err = ErrLeaderOutDated
			res.Value = ""
			return
		}
	}

}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	opArgs := &Op{
		OpType:     OPGet,
		Seq:        args.Seq,
		Key:        args.Key,
		Identifier: args.Identifier,
	}
	res := kv.HandleOp(opArgs)
	//fmt.Printf("SERVER : Get key = %v, value = %v\n", args.Key, res.Value)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if args.Op == "Put" {
		kv.Put(args, reply)
	} else {
		kv.Append(args, reply)
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	opArgs := &Op{
		OpType:     OPPut,
		Seq:        args.Seq,
		Key:        args.Key,
		Val:        args.Value,
		Identifier: args.Identifier,
	}

	res := kv.HandleOp(opArgs)
	//fmt.Printf("SERVER : PUT key = %v, value = %v\n", args.Key, args.Value)
	reply.Err = res.Err
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	opArgs := &Op{
		OpType:     OPAppend,
		Seq:        args.Seq,
		Key:        args.Key,
		Val:        args.Value,
		Identifier: args.Identifier,
	}

	res := kv.HandleOp(opArgs)
	//fmt.Printf("SERVER : APPEND key = %v, value = %v\n", args.Key, args.Value)
	reply.Err = res.Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) DBExecute(op *Op) (res result) {
	res.LastSeq = op.Seq
	switch op.OpType {
	case OPGet:
		val, exist := kv.db[op.Key]
		//fmt.Printf("DBExecute GET : key = %v, value = %v\n", op.Key, val)
		if exist {
			res.Value = val
			return
		} else {
			res.Err = ErrNoKey
			res.Value = ""
			return
		}
	case OPPut:
		//fmt.Printf("DBExecute PUT : key = %v, value = %v\n", op.Key, op.Val)
		kv.db[op.Key] = op.Val
		return
	case OPAppend:
		val, exist := kv.db[op.Key]
		//fmt.Printf("DBExecute APPEND : key = %v, prev value = %v, appendval = %v\n", op.Key, val, op.Val)
		if exist {
			kv.db[op.Key] = val + op.Val
			return
		} else {
			kv.db[op.Key] = op.Val
			return
		}
	}
	return
}

func (kv *KVServer) ApplyHandler() {
	for !kv.killed() {
		log := <-kv.applyCh

		if log.CommandValid {
			op := log.Command.(Op)
			//fmt.Printf("ApplyHandler : op = %v\n", op)
			kv.mu.Lock()

			if log.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}

			kv.lastApplied = log.CommandIndex

			var res result

			needApply := false
			if hisMap, exist := kv.historyMap[op.Identifier]; exist {
				if hisMap.LastSeq == op.Seq {
					res = *hisMap
				} else if hisMap.LastSeq < op.Seq {
					needApply = true
				}
			} else {
				needApply = true
			}

			if needApply {
				// fmt.Printf("ApplyHandler : op = %v\n", op)
				// fmt.Printf("ApplyHandler : Command = %v\n", log.Command)
				res = kv.DBExecute(&op)
				res.ResTerm = log.SnapshotTerm
				kv.historyMap[op.Identifier] = &res
			}

			// Leader还需要额外通知handler处理clerk回复
			ch, exist := kv.waiCh[log.CommandIndex]
			if exist {
				kv.mu.Unlock()
				// 发送消息
				func() {
					defer func() {
						if recover() != nil {
							// 如果这里有 panic，是因为通道关闭
							fmt.Printf("leader %v ApplyHandler: 发现 identifier %v Seq %v 的管道不存在, 应该是超时被关闭了", kv.me, op.Identifier, op.Seq)
						}
					}()
					res.ResTerm = log.SnapshotTerm

					*ch <- res
				}()
				kv.mu.Lock()
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate/100*95 {
				snapShot := kv.GenSnapShot()
				kv.rf.Snapshot(log.CommandIndex, snapShot)
			}
			kv.mu.Unlock()

		} else if log.SnapshotValid {
			kv.mu.Lock()
			if log.SnapshotIndex >= kv.lastApplied {
				kv.LoadSnapShot(log.Snapshot)
				kv.lastApplied = log.SnapshotIndex
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) GenSnapShot() []byte {
	// 调用时必须持有锁mu
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.db)
	e.Encode(kv.historyMap)

	serverState := w.Bytes()
	return serverState
}

func (kv *KVServer) LoadSnapShot(snapShot []byte) {
	// 调用时必须持有锁mu
	if len(snapShot) == 0 || snapShot == nil {
		//fmt.Printf("server %v LoadSnapShot: 快照为空", kv.me)
		return
	}

	r := bytes.NewBuffer(snapShot)
	d := labgob.NewDecoder(r)

	tmpDB := make(map[string]string)
	tmpHistoryMap := make(map[int64]*result)
	if d.Decode(&tmpDB) != nil ||
		d.Decode(&tmpHistoryMap) != nil {
		//fmt.Printf("server %v LoadSnapShot 加载快照失败\n", kv.me)
	} else {
		kv.db = tmpDB
		kv.historyMap = tmpHistoryMap
		// fmt.Printf("server %v LoadSnapShot 加载快照成功\n", kv.me)
	}
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	kv.historyMap = make(map[int64]*result)
	kv.db = make(map[string]string)
	kv.waiCh = make(map[int]*chan result)

	kv.mu.Lock()
	kv.LoadSnapShot(persister.ReadSnapshot())
	kv.mu.Unlock()

	go kv.ApplyHandler()
	// You may need initialization code here.

	return kv
}
