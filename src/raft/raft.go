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

import "sync"
import "labrpc"
import "time"
import "math/rand"

//import "strconv"

import "bytes"
import "encoding/gob"
import "fmt"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
        
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
		mu        sync.Mutex
		peers     []*labrpc.ClientEnd
		persister *Persister
		me        int // index into peers[]
        currentTerm int
        votedFor int
        votecount int
        logs       []Log
        commitIndex int
        lastApplied int
        nextIndex  []int
        BecomeLeader chan bool
        Heartbeat chan bool
        state string      
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	     // used in candidate state, reset everytime when became a candidate
}

type Log struct {
	COMMAND interface{}
	TERM   int
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var isleader bool
	// Your code here.
	if rf.state == "leader" {
		isleader = true
	} else {
		isleader = false
	}
	return rf.currentTerm, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.logs)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.nextIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.logs)
	d.Decode(&rf.commitIndex)
	d.Decode(&rf.lastApplied)
	d.Decode(&rf.nextIndex)
	rf.state = "follower"
	rf.votedFor = -1
	if len(rf.logs) == 0 {
		firstLog := new(Log) // initialize all nodes' Logs
		rf.logs = []Log{*firstLog}
	}
	// println("rf.me " + strconv.Itoa(rf.me) + " : In readPersist, len(rf.logs): " + strconv.Itoa(len(rf.Logs)) + " Term: " + strconv.Itoa(rf.CurrentTerm) + " CommitIndex: " + strconv.Itoa(rf.CommitIndex))
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	TERM        int
	CANDIDATEID int
	LASTLOGIDX  int
	LASTLOGTERM int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	TERM        int
	VOTEGRANTED bool
}

//
// example RequestVote RPC handler.
//
 
func (rf *Raft) RequestVote(args* RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	// Vote granted in following cases:
	// 1. candidate's term is more up-to-date
	// 2. candidate's term is equal to voter's term "AND" one of the following condition satisfied:
	// 		* voter has not voted yet
	// 		* voter has voted candidate at least once. (since each voter can only vote for one guy in each term)
	// 3. Prevent a candidate from winning an election unless its log contains all committed entries.
	//		* the RPC includes information about the candidate's log, and the voter denies its vote if its own log              more
	//		  up-to-date than that of the candidate.

	// Raft determines which of two Logs is more up-to-date by comparing the index and the term of the last entries
	// in the log. If the Logs have same last entries with different terms, then the log with the later term is
	// more up-to-date. If the Logs end with the same term, then whichever log is longer is more up-to-date
	
	// If votedFor is null or candidateId, and candidate’s log is at
	//least as up-to-date as receiver’s log, grant vote

	// the RPC includes information about the candidate’s log, and the
	//voter denies its vote if its own log is more up-to-date than
	//that of the candidate
	fmt.Printf("%v", rf.me)
	fmt.Printf(":request vote\n")	
	if args.TERM < rf.currentTerm {
		 reply.VOTEGRANTED = false
		 reply.TERM = rf.currentTerm
		 return
	}

     uptodate := Uptodate(rf.currentTerm , len(rf.logs) - 1, args.LASTLOGTERM, args.LASTLOGIDX)
     if uptodate == true && rf.votedFor == -1 {
     	rf.votedFor = args.CANDIDATEID
     	reply.VOTEGRANTED = true
     	reply.TERM = args.TERM
     	rf.currentTerm = args.TERM
     } else {
     	reply.VOTEGRANTED = false
     	reply.TERM = args.TERM
     } 
	    
}

// return true if candidate's log is more up-to-date

func Uptodate(rfterm int, rfindex int, argterm int, argindex int) bool{
	  if argterm == rfterm {
	  	 return argindex >= rfindex 
	  }
	  return argterm >= rfterm
         
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	// Calling peer node indefinitely until the other side has response (for loop).
	// As long as the other side has response, check if it has granted for the candidate,
	// if so then check if candidate's voteCount has reached majority, if so then switch to "leader" state
	   fmt.Printf("%v", rf.me)
	     fmt.Printf(":send request vote\n")	
	   rf.mu.Lock()	
	   rf.votecount = 1
	   rf.mu.Unlock()
	   ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	   if ok { 
	   	       if reply.TERM > rf.currentTerm {
               	       rf.mu.Lock()
               	       rf.state = "follower"
               	       rf.mu.Unlock()
               } else if reply.VOTEGRANTED == true {
               	  rf.mu.Lock()
               	  rf.votecount = rf.votecount + 1
               	  if rf.votecount > len(rf.peers) / 2 {
                      rf.BecomeLeader <- true
               	  }
               	   rf.mu.Unlock()
               } 
	   }
	   return ok
}

// send RequestVote to all other nodes in the cluster
func (rf *Raft) BroadcastRequestVote() {
	  fmt.Printf("%v", rf.me)
	  fmt.Printf(":broadcast request vote\n")		
      for i := 0; i < len(rf.peers); i++ {
             if i != rf.me && rf.state == "candidate" {
                    rf.mu.Lock()
                    args := &RequestVoteArgs{}
                    args.TERM = rf.currentTerm
	 				args.CANDIDATEID = rf.me
					args.LASTLOGIDX  = len(rf.logs) - 1
					args.LASTLOGTERM = rf.logs[len(rf.logs) - 1].TERM
                    reply := &RequestVoteReply{}
                    rf.mu.Unlock()
                    go rf.sendRequestVote(i, args, reply)
             }

      }
}

// used by leader of the cluster

type AppendEntries struct {
	 TERM int
	 LEADERID int
	 PREVLOGINDEX int
	 PREVLOGTERM int
	 ENTRIES Log
	 LEADERCOMMIT int
}

type AppendEntriesReply struct {
	TERM int
	SUCCESS bool

}

func (rf *Raft) AppendEntriesRPC(args *AppendEntries, reply *AppendEntriesReply) {
	            fmt.Printf("%v", rf.me)
	            fmt.Printf(":append entries\n")	
	            if args.TERM < rf.currentTerm {
	            	reply.SUCCESS = false
	            	reply.TERM = rf.currentTerm
	            	return
	            }

                rf.Heartbeat <- true    

}

func (rf *Raft) sendAppendEntriesRPC(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	// Calling peer node indefinitely until the other side has response (for loop).
	// As long as the other side has response, check if it has accepted as a leader,
	// if not, check if leader's term is up-to-date, if not, step down to follower
	     fmt.Printf("%v", rf.me)
	     fmt.Printf(":send append entries\n")	
        ok := rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)
		return ok
}

// send AppendEntriesRPC to all other nodes in the cluster
func (rf *Raft) BroadcastAppendEntriesRPC() {
	   fmt.Printf("%v", rf.me)
	   fmt.Printf(":broadcast append entries\n")
	   for i := 0; i < len(rf.peers); i++ {
	   	    if i != rf.me && rf.state == "leader" {
	   	    	args := &AppendEntries{}
	   	    	args.TERM = rf.currentTerm
	   	    	args.LEADERID = rf.me
	   	    	args.PREVLOGTERM = len(rf.logs) - 1
	   	    	args.ENTRIES = rf.logs[len(rf.logs) - 1]
	   	    	args.PREVLOGINDEX = rf.logs[len(rf.logs) - 1].TERM
	   	    	reply := &AppendEntriesReply{}
	   	    	go rf.sendAppendEntriesRPC(i, args, reply)
	   	    	
	   	    }
	   }         
}

// leader update CommitIndex
func (rf *Raft) UpdateCommit() {


}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    
         index := -1
	       term := -1
	       isLeader := true

	       // Your code here (2B).


	       return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}



// feed newly committed commands into state machine
func (rf *Raft) FeedStateMachine(applyCh chan ApplyMsg) {
	
}

func (rf *Raft) CandidateState() {
	// send RequestVoteRPC to all other servers, retry until:
	// 1. Receive votes from majority of servers
	// 		* Become leader
	// 		* Send AppendEntries heartbeat to all other servers, aka: change to "leader" and back to main Loop()
	// 2. Receive RPC from valid leader
	//		* Return to "follower" state
	// 3. No-one win election(election timeout elapses)
	// 		* Increment term, start new election

	// send RequestVote to all other nodes, and wait for BecomeLeaderCH
	fmt.Printf("%v", rf.me)
	fmt.Printf(":candidatestate\n")
    rf.currentTerm += 1
	rf.BroadcastRequestVote()
    select {
    	case <-rf.BecomeLeader :
    		 rf.mu.Lock()
    		 rf.state = "leader"
    		 rf.mu.Unlock()
    	case <- rf.Heartbeat:
    	     rf.mu.Lock()
    	     rf.state = "follower"
    	     rf.mu.Unlock()
        case<- time.After(time.Duration(600) * time.Millisecond):
        	 rf.mu.Lock()
        	 rf.state = "follower"
        	 rf.mu.Unlock()
    }
	
                
		// candidate state continues until a) it wins the
		// election, (b) another server establishes itself as leader, or
		// (c) a period of time goes by with no winner
	
}

func (rf *Raft) LeaderState() {
	fmt.Printf("%v", rf.me)
	fmt.Printf(":leaderstate\n")
	rf.BroadcastAppendEntriesRPC()
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// initialize from state persisted before a crash
         rf := &Raft{}
         rf.peers = peers
         rf.me = me
         rf.currentTerm = 0
         rf.votedFor = -1
         tmp := Log{}
         tmp.TERM = 0
         rf.logs = append(rf.logs, tmp)
         rf.commitIndex = 0
         rf.lastApplied = 0
         rf.BecomeLeader = make(chan bool)
         rf.Heartbeat = make(chan bool) 
         rf.state = "follower"

         go func() {
         	for {
         		if rf.state == "follower" {
            		select {
              			case <-rf.Heartbeat :
	           		// normal 
              			fmt.Printf("%v", rf.me)
	                    fmt.Printf(":do follower\n")
              			case <-time.After(time.Duration(300 + rand.Int31n(600)) * time.Millisecond):
                      	rf.mu.Lock()
                      	rf.state = "candidate"
                      	rf.mu.Unlock()
            		}
            	} else if rf.state == "candidate" {
            		    fmt.Printf("%v", rf.me)
	                    fmt.Printf(":do candidate\n")
              			rf.CandidateState()
         		} else if rf.state == "leader" {
         			    fmt.Printf("%v", rf.me)
	                    fmt.Printf(":do leader\n")
              			rf.LeaderState()
         		}
         	}
         }()
         return rf
} 
