package raft

//                   _ooOoo_
//                  o8888888o
//                  88" . "88
//                  (| -_- |)
//                  O\  =  /O
//               ____/`---'\____
//             .'  \\|     |//  `.
//            /  \\|||  :  |||//  \
//           /  _||||| -:- |||||-  \
//           |   | \\\  -  /// |   |
//           | \_|  ''\---/''  |   |
//           \  .-\__  `-`  ___/-. /
//         ___`. .'  /--.--\  `. . __
//      ."" '<  `.___\_<|>_/___.'  >'"".
//     | | :  `- \`.;`\ _ /`;.`/ - ` : | |
//     \  \ `-.   \_ __\ /__ _/   .-` /  /
//======`-.____`-.___\_____/___.-`____.-'======
//                   `=---='
//
//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//          佛祖保佑           永无BUG
//         God Bless        Never Crash

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

type LogEntry struct {
	Command      interface{}
	CommandIndex int
	CommandTerm  int
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
	votedFor    int
	logs        []LogEntry
	state       int

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// some utils
	electionTimeout  time.Time
	heartbeatTimeout time.Time
	voteCount        int
	applyCh          chan ApplyMsg
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
	return len(rf.logs) - 1
}

func (rf *Raft) initLeaderStates() {
	// init nextIndex and matchIndex every time when server become a leader
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs)
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
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
	Term         int
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm (§5.1)
		reply.Success = false
	} else if rf.logs[args.PrevLogIndex].CommandTerm != args.PrevLogTerm {
		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		reply.Success = false
	} else {
		reply.Success = true
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.resetElectionTimeout()
		if len(args.Entries) != 0 {
			DPrintf("server %v received Append Entry from server %v with term %v and command %v", rf.me, args.LeaderId, args.Term, args.Entries)
			// If an existing entry conflicts with a new one (same index but different
			// terms), delete the existing entry and all that follow it (§5.3)
			// Append any new entries not already in the log
			curIdx := args.PrevLogIndex + 1
			if len(rf.logs) > (curIdx) && rf.logs[curIdx].CommandTerm != args.Term {
				rf.logs = rf.logs[:curIdx+1]
			}
			rf.logs = append(rf.logs, args.Entries...)
		}
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			commitIdx := min(args.LeaderCommit, len(rf.logs)-1)
			// for tester purpose
			for i := rf.commitIndex + 1; i <= commitIdx; i++ {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i].Command,
					CommandIndex: i,
				}
			}
			rf.commitIndex = commitIdx
		}
	}
	rf.mu.Unlock()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
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
	if !ok {
		return ok
	}
	rf.mu.Lock()
	if rf.state == LEADER || args.Term != rf.currentTerm {
		DPrintf("Already become learder or received vote from last term")
		rf.mu.Unlock()
		return false
	}
	if reply.VoteGranted {
		rf.voteCount++
	}
	// case: C->L: receives votes from majority of servers
	if rf.voteCount > (len(rf.peers) / 2) {
		DPrintf("server %v become a leader with term %v", rf.me, rf.currentTerm)
		rf.voteCount = -1
		rf.state = LEADER
		rf.initLeaderStates()
		// send first HB
		rf.appendEntryBroadcast(true)
	}
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) ifLeaderUpdateCommitIdx(n int) {
	// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
	// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)
	// This function should be called when mutex is locked
	var matchCount int
	if n <= rf.commitIndex {
		return
	}
	for _, matchIdx := range rf.matchIndex {
		if matchIdx >= n {
			matchCount += 1
		}
	}
	if matchCount <= (len(rf.peers) / 2) {
		return
	}
	if rf.logs[n].CommandTerm != rf.currentTerm {
		return
	}
	// for tester usage
	for i := rf.commitIndex + 1; i <= n; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: rf.logs[i].CommandIndex,
		}
		rf.applyCh <- msg
	}
	rf.commitIndex = n
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return ok
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Success {
		// If successful: update nextIndex and matchIndex for follower (§5.3)
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.ifLeaderUpdateCommitIdx(rf.matchIndex[server])
	} else {
		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)

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
	// If command received from client: append entry to local log
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.lastLogIndex() + 1
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	// Your code here (2B).
	if isLeader {
		log := LogEntry{
			Command:      command,
			CommandIndex: index,
			CommandTerm:  term,
		}
		rf.logs = append(rf.logs, log)
		DPrintf("server %v receive user command %v", rf.me, command)
		rf.appendEntryBroadcast(false) // notify other servers
	}

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
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}
			reply := RequestVoteReply{}
			// sending request asynchronously
			go rf.sendRequestVote(server_id, &args, &reply)
		}
	}
}

func (rf *Raft) appendEntryBroadcast(hb bool) {
	// You should lock the mutex before calling this function
	for server_id, _ := range rf.peers {
		if server_id != rf.me {
			entries := []LogEntry{}
			if !hb {
				// If last log index ≥ nextIndex for a follower: send
				// AppendEntries RPC with log entries starting at nextIndex
				if rf.lastLogIndex() < rf.nextIndex[server_id] {
					continue
				} else {
					entries = append(entries, rf.logs[rf.nextIndex[server_id]])
				}

			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[server_id] - 1,
				PrevLogTerm:  rf.logs[rf.nextIndex[server_id]-1].CommandTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			if !hb {
				DPrintf("server %v send Append Entry to server %v", rf.me, server_id)
			}
			go rf.sendAppendEntries(server_id, &args, &reply)
		}
	}
	if hb {
		rf.resetHeartbeatTimeout()
	}
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
				rf.appendEntryBroadcast(true)
			}
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 10
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
	// For 2A
	rf.resetElectionTimeout()
	rf.state = FOLLOWER
	rf.currentTerm = 1
	rf.votedFor = -1
	// For 2B
	rf.logs = make([]LogEntry, 0)
	rf.logs = append(rf.logs, LogEntry{}) // append empty entry
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
