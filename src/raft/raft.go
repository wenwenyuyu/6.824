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
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type Entry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       State
	currentTerm int
	votedFor    int

	log         []Entry
	commitIndex int // 已经被提交的最大日志条目的索引
	lastApplied int // 已经被应用到状态机的最高的日志条目的索引

	nextIndex  []int // 乐观估计，对于每一台服务器，发送到该服务器的下一个日志条目的索引
	matchIndex []int // 悲观估计，对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引

	electionTimeOut int
	heartBeats      time.Time
	votedCount      int

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	snapShot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int
}

// log中的偏移
func (rf *Raft) RealLogIdx(vIdx int) int {
	return vIdx - rf.lastIncludedIndex
}

// 全局偏移
func (rf *Raft) VirtualLogIdx(rIdx int) int {
	return rIdx + rf.lastIncludedIndex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)

	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapShot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var votedFor int
	var currentTerm int
	var log []Entry
	var lastIncludeTerm int
	var lastIncludedIndex int

	if d.Decode(&votedFor) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
	} else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludeTerm

		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	// 目前只在Make中调用, 因此不需要锁
	if len(data) == 0 {
		//fmt.Printf("server %v 读取快照失败: 无快照\n", rf.me)
		return
	}
	rf.snapShot = data
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex < index || index <= rf.lastIncludedIndex {
		return
	}

	//fmt.Printf("server %v 同意了 Snapshot 请求, 其index=%v, 自身commitIndex=%v, 原来的lastIncludedIndex=%v, 快照后的lastIncludedIndex=%v\n", rf.me, index, rf.commitIndex, rf.lastIncludedIndex, index)
	rf.snapShot = snapshot
	rf.lastIncludedTerm = rf.log[rf.RealLogIdx(index)].Term

	rf.log = rf.log[rf.RealLogIdx(index):]
	rf.lastIncludedIndex = index
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type RequestAppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // 紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int // 紧邻新日志条目之前的那个日志条目的任期
	Entries      []Entry
	LeaderCommit int // 领导人的已知已提交的最高的日志条目的索引
}

type RequestAppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if args.term < rf.currentTerm; return false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.heartBeats = time.Now()
	// 遇到新的leader；更新自己的任期
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}

	// 心跳和日志复制不能分开处理，可能会出现leader断线一直发心跳处理
	// 此时已经选出了新的leader
	// 如果像这边这样处理的话，旧leader就不知道他已经不是leader了
	// if len(args.Entries) == 0 {
	// 	//fmt.Printf("%v 接收到心跳\n", rf.me)
	// 	rf.heartBeats = time.Now()
	// 	if args.LeaderCommit > rf.commitIndex {
	// 		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
	// 		//fmt.Printf("%v commitIndex : %v, lastApplied : %v\n", rf.me, rf.commitIndex, rf.lastApplied)
	// 	}
	// 	reply.Term = rf.currentTerm
	// 	reply.Success = true
	// 	return
	// }

	// len(rf.log)代表该raft的下一个应该接受的log
	// xterm == -1代表没有找到该index
	// xlen代表该raft的下一个应该接受的log
	// leader需要直接回退到xlen
	if args.PrevLogIndex >= rf.VirtualLogIdx(len(rf.log)) {
		reply.XTerm = -1
		reply.XLen = rf.VirtualLogIdx(len(rf.log))
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// index中的prevLogTerm不一致
	// 快速回退
	// 找到该term在log中第一次出现的index
	if rf.log[rf.RealLogIdx(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[rf.RealLogIdx(args.PrevLogIndex)].Term
		i := args.PrevLogIndex
		for i > rf.lastIncludedIndex && rf.log[rf.RealLogIdx(i)].Term == reply.XTerm {
			i -= 1
		}
		// term的第一个index
		reply.XIndex = i + 1
		reply.XLen = rf.VirtualLogIdx(len(rf.log))
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 找到prevLogIndex 但是 发生了冲突
	// 例如 rf在prevLogIndex之后有新的log，需要删除
	// prevLogIndex为10 leader的新term为8
	// 要删除11之后的日志
	// 1 2 3 4 5 6 7 8 9 10 11 12
	// 1 1 1 4 4 5 5 6 6 6  8
	// 1 1 1 4 4 5 5 6 6 6  6
	// 1 1 1 4 4 5 5 6 6 6  7  7
	//fmt.Printf("%v 需要新增log entry\n", rf.me)
	// for idx, log := range args.Entries {
	// 	ridx := args.PrevLogIndex + 1 + idx
	// 	if ridx < len(rf.log) && rf.log[ridx].Term != log.Term {
	// 		// 某位置发生了冲突, 覆盖这个位置开始的所有内容
	// 		rf.log = rf.log[:ridx]
	// 		rf.log = append(rf.log, args.Entries[idx:]...)
	// 		//rf.persist()
	// 		break
	// 	} else if ridx == len(rf.log) {
	// 		// 没有发生冲突但长度更长了, 直接拼接
	// 		rf.log = append(rf.log, args.Entries[idx:]...)
	// 		//rf.persist()
	// 		break
	// 	}
	// }
	// 无论一不一样，全部删除
	// [ ... 7] 7 8 [9 10]
	// [ ... 7] 7 8 9 10
	// prevLogIndex == 8
	// if server已经有了9 10，则进行删除
	// len(rf.log) 代表server的下一个log 如果大于9，说明9已经有了，直接删除
	if len(args.Entries) != 0 && rf.VirtualLogIdx(len(rf.log)) > args.PrevLogIndex+1 {
		rf.log = rf.log[:rf.RealLogIdx(args.PrevLogIndex+1)]
	}

	// 添加log
	rf.log = append(rf.log, args.Entries...)
	rf.persist()
	//fmt.Printf("%v 的log为 %v, index = %v\n", rf.me, rf.log, len(rf.log)-1)
	// 说明增加成功
	reply.Success = true
	reply.Term = rf.currentTerm

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.VirtualLogIdx(len(rf.log)-1) {
			rf.commitIndex = rf.VirtualLogIdx(len(rf.log) - 1)
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		//fmt.Printf("%v commitIndex : %v, lastApplied : %v\n", rf.me, rf.commitIndex, rf.lastApplied)
		// 唤醒
		rf.applyCond.Signal()
	}
}

// leader为每个follower发送心跳
func (rf *Raft) SendHeartBeats() {
	for !rf.killed() {
		if rf.state != Leader {
			return
		}

		//fmt.Printf("%v 开始发送心跳\n", rf.me)

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			// len(rf.log) 代表下一个log的序列
			// len(rf.log) - 1代表log目前的index
			// rf.nextIndex[i]代表成为leader时，leader的下一个log的index
			// 什么时候更新log? leader的log有增加，即len(log) - 1 >= rf.nextIndex[i]时
			// 其他情况都说明是成为leader
			args := &RequestAppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				LeaderCommit: rf.commitIndex,
			}

			sendSnapShot := false
			//fmt.Printf("%v 的最新log为 %v, %v 的 nextIndex为%v\n", rf.me, len(rf.log)-1, i, rf.nextIndex[i])
			if rf.lastIncludedIndex > args.PrevLogIndex {
				//fmt.Printf("%v need to send snap shot to %v, PrevLogIndex = %v, leaderCommitIndex = %v, logs = %v\n", rf.me, i, args.PrevLogIndex, rf.commitIndex, rf.log)
				sendSnapShot = true
			} else if rf.VirtualLogIdx(len(rf.log)-1) > args.PrevLogIndex {
				//fmt.Printf("%v need to send log Entries to %v, PrevLogIndex = %v, leaderCommitIndex = %v, logs = %v\n", rf.me, i, args.PrevLogIndex, rf.commitIndex, rf.log)
				args.Entries = rf.log[rf.RealLogIdx(args.PrevLogIndex+1):]
			} else {
				//fmt.Printf("%v need to send heart beats to %v, PrevLogIndex = %v, leaderCommitIndex = %v, logs = %v\n", rf.me, i, args.PrevLogIndex, rf.commitIndex, rf.log)
				args.Entries = make([]Entry, 0)
			}
			rf.mu.Unlock()

			if sendSnapShot {
				go rf.HandleInstallSnapShot(i)
			} else {
				args.PrevLogTerm = rf.log[rf.RealLogIdx(args.PrevLogIndex)].Term
				go rf.WaitHeartBeatsReply(i, args)
			}
		}

		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

// leader处理每个返回的心跳结果
func (rf *Raft) WaitHeartBeatsReply(server int, args *RequestAppendEntriesArgs) {
	reply := &RequestAppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Printf("%v 获得了 %v 心跳返回消息, %v的term = %v,自己的term为%v\n", rf.me, server, server, reply.Term, rf.currentTerm)
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.heartBeats = time.Now()
		rf.persist()
		return
	}

	// if len(args.Entries) == 0 {
	// 	return
	// }
	if args.Term != rf.currentTerm {
		return
	}

	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		N := rf.VirtualLogIdx(len(rf.log) - 1)
		for N > rf.commitIndex {
			count := 1
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N && rf.log[rf.RealLogIdx(N)].Term == rf.currentTerm {
					count += 1
				}
			}

			if count > len(rf.peers)/2 {
				break
			}
			N -= 1
		}
		rf.commitIndex = N
		rf.applyCond.Signal()
		//fmt.Printf("%v leader change commitIndex = %v, server = %v\n", rf.me, rf.commitIndex, server)
		return
	}

	if reply.Term > rf.currentTerm {
		// 回复了更新的term, 表示自己已经不是leader了

		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		// rf.timeStamp = time.Now()
		rf.heartBeats = time.Now()
		rf.persist()
		return
	}

	if reply.Term == rf.currentTerm && rf.state == Leader {
		// term == -1，说明prevLogIndex太大了
		// 将nextIndex回退到 xlen的位置
		// xlen <= rf.lastIncludedIndex 说明需要发送闪照了
		// 否则，nextIndex回退到xLen的位置
		if reply.XTerm == -1 {
			if rf.lastIncludedIndex >= reply.XLen {
				go rf.HandleInstallSnapShot(server)
			} else {
				rf.nextIndex[server] = reply.XLen
			}
			return
		}

		// 否则需要跳到Term的第一个index
		i := rf.nextIndex[server] - 1
		if i < rf.lastIncludedIndex {
			i = rf.lastIncludedIndex
		}

		// 出界或者找到term了
		for i > rf.lastIncludedIndex && rf.log[rf.RealLogIdx(i)].Term > reply.XTerm {
			i -= 1
		}

		// 如果到lastIncludedIndex还没找到，说明需要发送闪照
		if i == rf.lastIncludedIndex && rf.log[rf.RealLogIdx(i)].Term > reply.XTerm {
			go rf.HandleInstallSnapShot(server)
		} else if rf.log[rf.RealLogIdx(i)].Term == reply.XTerm {
			// 如果该term == XTerm
			rf.nextIndex[server] = i + 1
		} else {
			// if term < xTerm
			// 就直接用reply的index吧
			if reply.XIndex <= rf.lastIncludedIndex {
				// XIndex位置也被截断了
				// 添加InstallSnapshot的处理
				go rf.HandleInstallSnapShot(server)
			} else {
				rf.nextIndex[server] = reply.XIndex
			}
		}

		return
	}

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()

	//fmt.Printf("%d开始投票\n", rf.me)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.persist()
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.VirtualLogIdx(len(rf.log)-1)) {
			//fmt.Printf("%d投赞成票\n", rf.me)
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.state = Follower
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			rf.heartBeats = time.Now()
			rf.votedCount = 0
			rf.persist()
			rf.mu.Unlock()
			return
		}
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	rf.mu.Unlock()
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3B).
	if rf.state != Leader {
		return -1, -1, false
	}

	entry := &Entry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, *entry)
	//fmt.Printf("%v leader receive a log : %v\n", rf.me, *entry)
	rf.persist()

	return rf.VirtualLogIdx(len(rf.log) - 1), rf.currentTerm, true
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 进行投票
func (rf *Raft) Elect() {
	rf.mu.Lock()
	//fmt.Printf("id = %d 开始选举\n", rf.me)
	rf.currentTerm += 1
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.votedCount = 1
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.VirtualLogIdx(len(rf.log) - 1),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.HandlerVoteResult(i, args)
	}
}

// 投票结果处理
func (rf *Raft) HandlerVoteResult(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		//fmt.Printf("%d没有收到%d的票\n", rf.me, server)
		return
	}
	//fmt.Printf("%d接收到%d的票, 投了%v票, term为%v\n", rf.me, server, reply.VoteGranted, reply.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}

	if !reply.VoteGranted {
		return
	}

	if rf.votedCount > len(rf.peers)/2 {
		return
	}

	//fmt.Printf("%v增加票数\n", rf.me)
	rf.votedCount++
	if rf.votedCount > len(rf.peers)/2 {
		//fmt.Printf("%v变成leader\n", rf.me)
		if rf.state != Candidate {
			return
		}
		rf.state = Leader
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = rf.VirtualLogIdx(len(rf.log))
			rf.matchIndex[i] = rf.lastIncludedIndex
		}

		go rf.SendHeartBeats()
	}
}

func (rf *Raft) CommittedCommand() {
	for !rf.killed() {
		rf.mu.Lock()
		// DPrintf("server %v CommitChecker 获取锁mu", rf.me)
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		msgBuf := make([]*ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		tmpApplied := rf.lastApplied
		for rf.commitIndex > tmpApplied {
			tmpApplied += 1
			if tmpApplied <= rf.lastIncludedIndex {
				// tmpApplied可能是snapShot中已经被截断的日志项, 这些日志项就不需要再发送了
				continue
			}
			msg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.RealLogIdx(tmpApplied)].Command,
				CommandIndex: tmpApplied,
				SnapshotTerm: rf.log[rf.RealLogIdx(tmpApplied)].Term,
			}

			msgBuf = append(msgBuf, msg)
		}
		rf.mu.Unlock()
		// DPrintf("server %v CommitChecker 释放锁mu", rf.me)

		// 注意, 在解锁后可能又出现了SnapShot进而修改了rf.lastApplied

		for _, msg := range msgBuf {
			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				continue
			}
			//DPrintf("server %v 准备commit, log = %v:%v, lastIncludedIndex=%v", rf.me, msg.CommandIndex, msg.SnapshotTerm, rf.lastIncludedIndex)

			rf.mu.Unlock()
			// 注意, 在解锁后可能又出现了SnapShot进而修改了rf.lastApplied
			// fmt.Printf("CommittedCommand: server = %v, msg = %v\n", rf.me, *msg)
			rf.applyCh <- *msg

			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				continue
			}
			rf.lastApplied = msg.CommandIndex
			rf.mu.Unlock()
		}

	}
}

// 心跳检测，检测是否应该进行投票
// 若上一次接收到心跳的时间点离现在的时间已经超过该raft的超时时间，就进行投票
// 睡300ms~500ms
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		// Your code here (3A)
		// Check if a leader election should be started.
		diff := time.Since(rf.heartBeats).Milliseconds()
		//fmt.Printf("index = %d,diff = %d,electionMs = %d,state = %v,term = %v\n",
		//	rf.me, diff, rf.electionTimeOut, rf.state, rf.currentTerm)
		rf.mu.Unlock()

		if rf.state != Leader && diff > int64(rf.electionTimeOut) {
			go rf.Elect()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.

		time.Sleep(time.Duration(rf.electionTimeOut) * time.Millisecond)
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	LastIncludedCmd   interface{}
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3D).
	//fmt.Printf("id = %d 收到快照\n", rf.me)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	rf.heartBeats = time.Now()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.state = Follower

	// 不需要实现offset
	// 如果现存的日志条目与快照中最后包含的日志条目具有相同的索引值和任期号，则保留其后的日志条目并进行回复
	// 丢弃整个日志
	// 使用快照重置状态机（并加载快照的集群配置）

	hasEntry := false
	i := 0
	for ; i < len(rf.log); i++ {
		if rf.VirtualLogIdx(i) == args.LastIncludedIndex && rf.log[i].Term == args.LastIncludedTerm {
			hasEntry = true
			break
		}
	}

	msg := &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	if hasEntry {
		rf.log = rf.log[i:]
	} else {
		rf.log = make([]Entry, 0)
		rf.log = append(rf.log, Entry{Term: args.LastIncludedTerm, Command: args.LastIncludedCmd})
	}

	rf.snapShot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	reply.Term = rf.currentTerm
	rf.applyCh <- *msg
	rf.persist()
}

func (rf *Raft) HandleInstallSnapShot(server int) {
	rf.mu.Lock()

	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapShot,
		LastIncludedCmd:   rf.log[0].Command,
	}

	reply := &InstallSnapshotReply{}
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
		return
	}

	rf.nextIndex[server] = rf.VirtualLogIdx(1)
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

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{Term: 0, Command: nil})

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.heartBeats = time.Now()
	source := rand.NewSource(int64(rf.me))
	r := rand.New(source)
	rf.electionTimeOut = 300 + r.Intn(50)
	rf.votedCount = 0

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	// initialize from state persisted before a crash
	rf.readSnapshot(persister.ReadSnapshot())
	rf.readPersist(persister.ReadRaftState())

	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.VirtualLogIdx(len(rf.log))
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.CommittedCommand()

	return rf
}
