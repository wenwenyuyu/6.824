/*
 * @Author       : wenwneyuyu
 * @Date         : 2024-09-20 16:41:46
 * @LastEditors  : wenwenyuyu
 * @LastEditTime : 2024-11-13 15:49:18
 * @FilePath     : /src/shardctrler/server.go
 * @Description  :
 * Copyright 2024 OBKoro1, All Rights Reserved.
 * 2024-09-20 16:41:46
 */
package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	table  map[int64]int64
	waitCh map[int]chan Err

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	IsQuery  bool
	Num      int
	Config   Config
	IsJoin   bool
	Servers  map[int][]string
	IsLeave  bool
	GIDs     []int
	IsMove   bool
	Shard    int
	GID      int
	ClientId int64
	SeqNo    int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// fmt.Printf("Join : args = %v\n", args)
	// Your code here.
	sc.mu.Lock()
	if args.SeqNo <= sc.table[args.ClientId] {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		IsJoin:   true,
		Servers:  args.Servers,
		ClientId: args.ClientId,
		SeqNo:    args.SeqNo,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	ch := make(chan Err, 1)

	sc.mu.Lock()
	sc.waitCh[index] = ch
	sc.mu.Unlock()

	select {
	case err := <-ch:
		reply.Err = err
		return
	case <-time.After(1000 * time.Millisecond):
		reply.Err = TimeOut
		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	// 将[]中的集群删除
	// fmt.Printf("Leave : args = %v\n", args)
	sc.mu.Lock()
	if args.SeqNo <= sc.table[args.ClientId] {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		IsLeave:  true,
		GIDs:     args.GIDs,
		ClientId: args.ClientId,
		SeqNo:    args.SeqNo,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	ch := make(chan Err, 1)

	sc.mu.Lock()
	sc.waitCh[index] = ch
	sc.mu.Unlock()

	select {
	case err := <-ch:
		reply.Err = err
		return
	case <-time.After(1000 * time.Millisecond):
		reply.Err = TimeOut
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// fmt.Printf("Move : args = %v\n", args)
	// Your code here.
	sc.mu.Lock()
	if args.SeqNo <= sc.table[args.ClientId] {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		IsMove:   true,
		Shard:    args.Shard,
		GID:      args.GID,
		ClientId: args.ClientId,
		SeqNo:    args.SeqNo,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	ch := make(chan Err, 1)

	sc.mu.Lock()
	sc.waitCh[index] = ch
	sc.mu.Unlock()

	select {
	case err := <-ch:
		reply.Err = err
		return
	case <-time.After(1000 * time.Millisecond):
		reply.Err = TimeOut
		return
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	// 返回第args.Num的config
	// 如果args.Num == -1 或者大于最大值，则返回最新的config
	// fmt.Printf("Query : args = %v\n", args)
	sc.mu.Lock()
	if args.Num >= 0 && args.Num < len(sc.configs) {
		reply.Config = sc.configs[args.Num]
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		IsQuery:  true,
		Num:      args.Num,
		ClientId: args.ClientId,
		SeqNo:    args.SeqNo,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	ch := make(chan Err, 1)
	sc.mu.Lock()
	sc.waitCh[index] = ch
	sc.mu.Unlock()

	select {
	case res := <-ch:
		if res == OK {
			sc.mu.Lock()
			// 返回最新的
			if args.Num == -1 || args.Num >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[args.Num]
			}
			reply.Err = res
			sc.mu.Unlock()
			return
		}
		reply.Err = res
		return
	case <-time.After(1000 * time.Millisecond):
		reply.Err = TimeOut
		return
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) doJoin(op Op) {
	// Join : args = &{map[1:[x y z]] 3780956679429522302 2}
	// Query : res = {1 [1 1 1 1 1 1 1 1 1 1] map[1:[x y z]]}
	// Join : args = &{map[2:[a b c]] 3780956679429522302 5}
	// Query : res = {2 [2 2 2 2 2 1 1 1 1 1] map[1:[x y z] 2:[a b c]]}
	// doJoin传入一个gid -> server的映射
	// 需要将原本的shared -> gid 重新分配，确保平衡
	// go中的map指向的是引用，需要进行拷贝赋值
	c := copyConfig(sc.configs[len(sc.configs)-1])
	c.Num++
	// map增加join内容
	for k, v := range op.Servers {
		c.Groups[k] = v
	}
	// fmt.Printf("DoJoin : c.Group = %v\n", c.Groups)
	// 首先得获得config中原来的gid，即Groups的key值
	// 接着拼接op.Servers的gid
	cid := make(map[int][]int)
	var gs []int
	// 获取所有的gid
	for gid := range c.Groups {
		cid[gid] = []int{}
		gs = append(gs, gid)
	}
	// 从小到大排序
	sort.Ints(gs)
	// fmt.Printf("DoJoin : gs = %v, cid = %v\n", gs, cid)
	// 获得每个gid对应的shards，就是将c.Shards的key,map相反
	// DoJoin : gs = [1 2], cid = map[1:[0 1 2 3 4 5 6 7 8 9] 2:[]]
	for i, v := range c.Shards {
		cid[v] = append(cid[v], i)
	}
	// fmt.Printf("DoJoin : gs = %v, cid = %v\n", gs, cid)
	maxi, maxv, mini, minv := maxmininMap(cid, gs)
	// fmt.Printf("DoJoin : maxi = %v, maxv = %v, mini = %v, minv = %v\n", maxi, maxv, mini, minv)
	// 进行动态平衡，确保每个gid的shards数量相差不超过1
	for maxv-minv > 1 {
		// 分一半给mini
		size := (maxv - minv) / 2
		arr := cid[maxi][:size]
		cid[mini] = append(cid[mini], arr...)
		cid[maxi] = cid[maxi][size:]
		for _, v := range arr {
			c.Shards[v] = mini
		}
		maxi, maxv, mini, minv = maxmininMap(cid, gs)
		// fmt.Printf("InLoop : maxi = %v, maxv = %v, mini = %v, minv = %v, shares = %v\n", maxi, maxv, mini, minv, c.Shards)
	}
	sc.configs = append(sc.configs, c)

}

func (sc *ShardCtrler) doLeave(op Op) {
	//println("doLeave %v at S%d", op.GIDs, sc.me)
	// 把一系列gid删除，然后做平衡
	c := copyConfig(sc.configs[len(sc.configs)-1])
	c.Num++
	for _, v := range op.GIDs {
		for shard, gid := range c.Shards {
			if gid == v {
				c.Shards[shard] = 0
			}
		}
		delete(c.Groups, v)
	}

	cid := make(map[int][]int) // gid -> shard
	var gs []int
	for gid := range c.Groups {
		cid[gid] = []int{}
		gs = append(gs, gid)
	}
	sort.Ints(gs)
	for i, v := range c.Shards {
		cid[v] = append(cid[v], i)
	}
	// fmt.Printf("DoLeave : gs = %v, cid = %v\n", gs, cid)
	maxi, maxv, mini, minv := maxmininMap(cid, gs)
	// fmt.Printf("DoLeave : maxi = %v, maxv = %v, mini = %v, minv = %v\n", maxi, maxv, mini, minv)
	// 注意一下，leave做balance时，是在zero区域做的
	var zero_arr []int
	for maxv-minv > 1 {
		size := (maxv - minv) / 2
		if maxi == 0 {
			zero_arr = cid[maxi][:size]
		}

		arr := zero_arr[:size]
		cid[mini] = append(cid[mini], arr...)
		cid[maxi] = zero_arr[size:]
		for _, v := range arr {
			c.Shards[v] = mini
		}
		maxi, maxv, mini, minv = maxmininMap(cid, gs)
		// fmt.Printf("InLoop : maxi = %v, maxv = %v, mini = %v, minv = %v, shares = %v\n", maxi, maxv, mini, minv, c.Shards)

	}
	sc.configs = append(sc.configs, c)
}

func (sc *ShardCtrler) doMove(op Op) {
	//println("doMove shard = %d,gid = %d at S%d\n", op.Shard, op.GID, sc.me)
	// 把op.shard移动到op.gid中，即更改一下shards的内容
	c := copyConfig(sc.configs[len(sc.configs)-1])
	c.Num++
	for i, _ := range c.Shards {
		if i == op.Shard {
			c.Shards[i] = op.GID
		}
	}
	sc.configs = append(sc.configs, c)
}

func (sc *ShardCtrler) isRepeated(clientId, seqNo int64) bool {
	value, ok := sc.table[clientId]
	if ok && value >= seqNo {
		return true
	}
	return false
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.table = make(map[int64]int64)
	sc.waitCh = make(map[int]chan Err)

	go sc.execute()
	return sc
}
