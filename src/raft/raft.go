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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor	int
	state	 	int

	// some utils
	electionTimeout time.Time
	heartbeatTimeout time.Time
	voteCount int

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

func (rf *Raft) lastLogTerm() int {
	// TODO
	return 0
}

func (rf *Raft) lastLogIndex() int {
	// TODO
	return 0
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term int
	LeaderId int // so follower can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm int // term of prevLogIndex entry
	LeaderCommit int // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term int // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("server %v received HB from server %v with term %v", rf.me, args.LeaderId, args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else {
		reply.Success = true
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		DPrintf("server %v reply HB succeed with current term %v", rf.me, rf.currentTerm)
		rf.resetElectionTimeout()
		DPrintf("server %v, election timout %v", rf.me, rf.electionTimeout.UnixMilli())
	}
	rf.mu.Unlock()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int // candidate's term
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Each server will vote for at most one candidate in a given term, on a first-come-first-served basis
	rf.mu.Lock()
	DPrintf("server %v receive voting request from server %v with term %v", rf.me, args.CandidateId, args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm || rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// check if candidate’s log is at least as up-to-date as receiver’s log
		if (rf.lastLogTerm() > args.LastLogTerm) || (rf.lastLogTerm() == args.LastLogTerm && rf.lastLogIndex() > args.LastLogIndex) {
			reply.VoteGranted = false
		} else {
			// case: C->F: discovers current leaders or new term
			// case: L->F: discover server with higher term
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.state = FOLLOWER
			rf.currentTerm = args.Term
			rf.resetElectionTimeout()
			DPrintf("server %v become a follower with term %v", rf.me, rf.currentTerm)
			// DPrintf("server %v, current Time %v", rf.me, time.Now().UnixMilli())
			// DPrintf("server %v, election timout %v", rf.me, rf.electionTimeout.UnixMilli())
		}
	} else {
		reply.VoteGranted = false
	}
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
	if ok {
		rf.mu.Lock()
		if rf.state == LEADER || args.Term != rf.currentTerm { 
			DPrintf("Already become learder or received vote from last term")
			rf.mu.Unlock()
			return false 
		}
		if reply.VoteGranted { rf.voteCount++ }
		// case: C->L: receives votes from majority of servers
		if rf.voteCount > (len(rf.peers) / 2) {
			DPrintf("server %v become a leader with term %v", rf.me, rf.currentTerm)
			rf.voteCount = -1
			rf.state = LEADER
			// send first HB
			rf.sendHeartbeat()
		}
		rf.mu.Unlock()
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		// TODO
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
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

func (rf *Raft) resetElectionTimeout() {
	ms := 500 + (rand.Int63() % 150)
	rf.electionTimeout = time.Now().Add(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) resetHeartbeatTimeout() {
	ms := 100
	rf.heartbeatTimeout = time.Now().Add(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) startElection() {
	rf.state = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.resetElectionTimeout()
	DPrintf("server %v start election with term %v, current time: %v", rf.me, rf.currentTerm, time.Now().UnixMilli())
	for server_id, _ := range rf.peers {
		if server_id != rf.me {
			args := RequestVoteArgs{
				Term: rf.currentTerm,
				CandidateId: rf.me,
			}
			reply := RequestVoteReply{}
			// sending request asynchronously
			go rf.sendRequestVote(server_id, &args, &reply)
		}
	}
}

func (rf *Raft) sendHeartbeat() {
	
	for server_id, _ := range rf.peers {
		if server_id != rf.me {
			args := AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
			}
			reply := AppendEntriesReply{}
			DPrintf("server %v send HB to server %v", rf.me, server_id)
			go rf.sendAppendEntries(server_id, &args, &reply)
		}
	}
	rf.resetHeartbeatTimeout()
	// DPrintf("server %v, current Time %v", rf.me, time.Now().UnixMilli())
	// DPrintf("server %v, HB timout %v", rf.me, rf.heartbeatTimeout.UnixMilli())
}

func (rf *Raft) ticker() {
	DPrintf("start a server with ID: %v", rf.me)
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		switch rf.state {
		case FOLLOWER, CANDIDATE:
			// case: F->C: time out, start election
			// case: C->C: time out, new election
			now := time.Now()
			if now.After(rf.electionTimeout) {
				// DPrintf("server %v start election, current time: %v, electionTimeout: %v", rf.me, time.Now(), rf.electionTimeout)
				rf.startElection()
			}
		case LEADER:
			// repeat during idle periods to prevent election timeouts (§5.2)
			now := time.Now()
			if now.After(rf.heartbeatTimeout) {
				rf.sendHeartbeat()
			}
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 30
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.resetElectionTimeout()
	rf.state = FOLLOWER
	rf.currentTerm = 1
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
