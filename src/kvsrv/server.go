package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	KVStores map[string]string
	KVCached map[int64]string
	KVCounts map[int64]uint64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//	fmt.Printf("Get Id = %c, key = %v, value = %v\n", args.ID, args.Key, kv.KVStores[args.Key])
	reply.Value = kv.KVStores[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, ok := kv.KVCounts[args.ID]
	if !ok {
		kv.KVCounts[args.ID] = 0
	}

	if args.Counts < kv.KVCounts[args.ID] {
		reply.Counts = kv.KVCounts[args.ID]
		return
	}

	kv.KVStores[args.Key] = args.Value
	kv.KVCounts[args.ID]++
	reply.Counts = kv.KVCounts[args.ID]
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, ok := kv.KVCounts[args.ID]
	if !ok {
		kv.KVCounts[args.ID] = 0
	}

	if args.Counts < kv.KVCounts[args.ID] {
		reply.Value = kv.KVCached[args.ID]
		reply.Counts = kv.KVCounts[args.ID]
		return
	}
	//fmt.Printf("Append Id = %v, key = %v, value = %v\n", args.ID, args.Key, args.Value)
	oldValue := kv.KVStores[args.Key]
	kv.KVStores[args.Key] = oldValue + args.Value
	reply.Value = oldValue
	kv.KVCached[args.ID] = oldValue
	kv.KVCounts[args.ID]++
	reply.Value = kv.KVCached[args.ID]
	reply.Counts = kv.KVCounts[args.ID]
}

func StartKVServer() *KVServer {
	kv := &KVServer{
		KVStores: make(map[string]string),
		KVCached: make(map[int64]string),
		KVCounts: make(map[int64]uint64),
	}

	// You may need initialization code here.

	return kv
}
