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

       //self define
        votecount int
        BecomeLeader chan bool
        Heartbeat chan bool
        Grantedvote chan bool
        state string
       // Persistent state
        CurrentTerm int
        votedFor int
        Logs       []Log
       //Volatile state on all servers:
        CommitIndex int
        LastApplied int

       //Volatile state on leaders:
        NextIndex  []int
        MatchIndex []int

        // Applying to service
	applyCh chan ApplyMsg
        
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	     // used in candidate state, reset everytime when became a candidate
}

type Log struct {
	COMMAND interface{}
	TERM   int
}

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


type AppendEntries struct {
	 TERM int
	 LEADERID int
	 PREVLOGINDEX int
	 PREVLOGTERM int
	 ENTRIES []Log
	 LEADERCOMMIT int
}

type AppendEntriesReply struct {
	TERM int
	SUCCESS bool
	NextIndex int
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
	return rf.CurrentTerm, isleader
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
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.Logs)
	//e.Encode(rf.CommitIndex)
	//e.Encode(rf.LastApplied)
	//e.Encode(rf.NextIndex)
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
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.Logs)
	d.Decode(&rf.CommitIndex)
	d.Decode(&rf.LastApplied)
	d.Decode(&rf.NextIndex)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
        rf.mu.Lock()
	defer rf.mu.Unlock() 
        term = rf.CurrentTerm
        if rf.state == "leader" {
           isLeader = true
           l := Log{}
           l.TERM = rf.CurrentTerm
           l.COMMAND = command
           rf.Logs = append(rf.Logs, l)
           index = len(rf.Logs) - 1
           fmt.Println(command, "leader append to self:", rf.me, len(rf.Logs) - 1)
           rf.persist()
           go rf.BroadcastAppendEntries()
        }
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

func (rf *Raft) BroadcastRequestVote() {
       //rf.mu.Lock()
       //defer rf.mu.Unlock()
       fmt.Println("in broadcastrequestvote")
       for i := 0; i < len(rf.peers); i++ {
             //fmt.Println("rf state", rf.state)
             if i != rf.me && rf.state == "candidate" {
	       go func(i int) {
                    fmt.Println("send candidate state")
                    args := &RequestVoteArgs{}
                    args.TERM = rf.CurrentTerm
	 	    args.CANDIDATEID = rf.me
		    args.LASTLOGIDX  = len(rf.Logs) - 1
		    args.LASTLOGTERM = rf.Logs[len(rf.Logs) - 1].TERM
                    reply := &RequestVoteReply{}
                    rf.sendRequestVote(i, args, reply)
               }(i)
             }
      }
}

func Uptodate(rfterm int, rfindex int, argterm int, argindex int) bool{
	  if argterm == rfterm {
	     return argindex >= rfindex
	  }
	  return argterm >= rfterm

}

func (rf *Raft) RequestVote(args* RequestVoteArgs, reply *RequestVoteReply) {
          rf.mu.Lock()
         defer rf.mu.Unlock()
         defer rf.persist()
         fmt.Println("in request vote")
         if args.TERM < rf.CurrentTerm {
                  //fmt.Println("return back")
                  reply.VOTEGRANTED = false
                  reply.TERM = rf.CurrentTerm
		  return
         }
        
         //fmt.Println(" go on")
         if rf.CurrentTerm < args.TERM {
		rf.state = "follower"
                fmt.Println(rf.me, "become follower, rf.currentterm < args.TERM")
		rf.CurrentTerm = args.TERM
                rf.votedFor = -1
	}
        reply.VOTEGRANTED = false
        reply.TERM = rf.CurrentTerm
        uptodate := Uptodate(rf.Logs[len(rf.Logs) - 1].TERM , len(rf.Logs) - 1, args.LASTLOGTERM, args.LASTLOGIDX)
        //fmt.Println("uptodate", uptodate)
        if uptodate == false {
           return
        }

        if uptodate && (rf.votedFor == -1 || rf.votedFor == args.CANDIDATEID) {
           //fmt.Println("in request vote reply true")
           rf.Grantedvote <- true
           reply.VOTEGRANTED = true
           rf.state = "follower"
           fmt.Println(rf.me, "become follower, vote for leader")
           rf.votedFor = args.CANDIDATEID
           return
        }
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
                
                fmt.Println("in send request vote")
                ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
                rf.mu.Lock()
	        defer rf.mu.Unlock()
                fmt.Println("after call requestvote")
                if ok {
                     if reply.VOTEGRANTED == false {
                         //fmt.Println("reply vote granted", reply.VOTEGRANTED)
                         if reply.TERM > rf.CurrentTerm{
                        	rf.state = "follower"
                                fmt.Println(rf.me, "become follower, in requestvote")
                        	rf.CurrentTerm = reply.TERM
                        	rf.votedFor = -1
                                rf.persist()
                      	 } 
	             } else {
                            rf.votecount = rf.votecount + 1
                            fmt.Println("rf.votecount", rf.votecount)
               	  	    if rf.votecount > len(rf.peers) / 2 && rf.state == "candidate" {
                                fmt.Println("become leader")
                      		rf.BecomeLeader <- true
               	  	    }               
                          
                      }
                }
                return ok
}

func (rf *Raft) sendAppendEntriesRPC(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
                //fmt.Println(" call appendentries")
                ok := rf.peers[server].Call("Raft.AppendEnriesRPC", args, reply)
               // fmt.Println("After call append  entries")
                rf.mu.Lock()
	        defer rf.mu.Unlock()
                if ok {
                      if reply.SUCCESS {
                             fmt.Println(server, "reply success", reply.SUCCESS)
                          if rf.state == "leader" {
                             rf.NextIndex[server] = args.PREVLOGINDEX + len(args.ENTRIES) + 1
                             rf.MatchIndex[server] = rf.NextIndex[server] - 1 
                             index := rf.MatchIndex[server]
                             count := 1
                             if index > rf.CommitIndex && rf.Logs[index].TERM == rf.CurrentTerm{
                                   for i := 0; i < len(rf.peers); i++ {
				      if rf.me != i && rf.MatchIndex[i] >= index {
					  count++
				       }
                                   }
                                   if count > len(rf.peers) / 2 {
						rf.CommitIndex = index
						go rf.CommitUpdate()
				   }
			     }
                          }
                      } else {
                              if reply.TERM > rf.CurrentTerm {
                                 rf.state = "follower"
                                 fmt.Println(rf.me, "become follower in sendappendentries")
                                 rf.CurrentTerm = reply.TERM
                                 rf.votedFor = -1 
                                 rf.persist()                              
                              } else {
                                 rf.NextIndex[server] = reply.NextIndex
                              }
                      }
                }
                return ok
}

func (rf *Raft) AppendEnriesRPC(args *AppendEntries, reply *AppendEntriesReply) {
                //fmt.Println("in append entries")
                rf.mu.Lock()
		defer rf.mu.Unlock()
               // defer rf.persist()
               
                if args.TERM < rf.CurrentTerm {
                fmt.Println(args.LEADERID, "'s term is :", args.TERM)
                fmt.Println(rf.me, "'s term is :",  rf.CurrentTerm)
                reply.SUCCESS = false
		reply.NextIndex = args.PREVLOGINDEX + 1
		reply.TERM = rf.CurrentTerm
		return
	        }
	        rf.Heartbeat <- true
                reply.TERM = args.TERM
	        if rf.CurrentTerm < args.TERM {
	            	rf.CurrentTerm = args.TERM
	            	rf.state = "follower"
                        fmt.Println(rf.me, "become follower in append entries")
	        } 
                 
                if len(rf.Logs) - 1 < args.PREVLOGINDEX {
                       reply.SUCCESS = false
                       reply.TERM = args.TERM
                       //reply.NextIndex = args.PREVLOGINDEX + 1
                       reply.NextIndex = len(rf.Logs)
                       return
                }

                if rf.Logs[args.PREVLOGINDEX].TERM != args.PREVLOGTERM {
                       reply.NextIndex = 1
                       for i := args.PREVLOGINDEX - 1; i >= 0; i-- {
                               if rf.Logs[args.PREVLOGINDEX].TERM != rf.Logs[i].TERM{
				  reply.NextIndex =  i + 1
                                  fmt.Println(rf.me, "reply next is :", reply.NextIndex)
				  break
                               }
			}
                       return
                }
                rf.Logs = rf.Logs[0 : args.PREVLOGINDEX + 1]
                for  i := 0; i < len(args.ENTRIES); i++ {
                		rf.Logs = append(rf.Logs, args.ENTRIES[i]) 
                                fmt.Println(args.TERM, "leader term", rf.CurrentTerm, "append to term")
                                fmt.Println(args.LEADERID , "leader:", args.ENTRIES[i], "append to :", rf.me, len(rf.Logs) - 1)

                }
                reply.NextIndex = len(rf.Logs)
		reply.SUCCESS = true
                rf.persist()
        	if args.LEADERCOMMIT > rf.CommitIndex {
                		 if args.LEADERCOMMIT < len(rf.Logs) - 1 {
                		 	rf.CommitIndex = args.LEADERCOMMIT
                		 } else {
                		 	rf.CommitIndex = len(rf.Logs) - 1
                		 }
                 fmt.Println("id, rf commitindex, rf len", rf.me, rf.CommitIndex,  len(rf.Logs))
                 go rf.CommitUpdate()
         	}
               
}

func (rf *Raft) CommitUpdate() {
      rf.mu.Lock()
      defer rf.mu.Unlock()
      base := rf.LastApplied + 1
      limit := rf.CommitIndex
      Logs := rf.Logs
      for i := base; i <= limit; i++ {
             if i < len(Logs) {
             msg := ApplyMsg{}
             msg.Index = i
            // fmt.Println("rf len", rf.me, len(rf.Logs), i)
	     msg.Command = Logs[i].COMMAND
	     rf.applyCh <- msg
             rf.LastApplied = i
             }
      }
}


func (rf *Raft) BroadcastAppendEntries() {
       rf.mu.Lock()
	defer rf.mu.Unlock()
     // fmt.Println("in broadcastappendentries:", rf.me)
       for i := 0; i < len(rf.peers); i++ {
	        //if i != rf.me && rf.state == "leader" {
                    go func(i int) {
                        if i != rf.me && rf.state == "leader" {
	   	    	args := &AppendEntries{}
	   	    	args.TERM = rf.CurrentTerm
	   	    	args.LEADERID = rf.me
	   	    	args.PREVLOGINDEX =  rf.NextIndex[i] - 1
                        //fmt.Println("args.PREVLOGINDEX", args.PREVLOGINDEX)
	   	    	args.PREVLOGTERM = rf.Logs[args.PREVLOGINDEX].TERM // easily wrong
	   	    	args.ENTRIES = rf.Logs[args.PREVLOGINDEX + 1 :] // easily wron
                        args.LEADERCOMMIT = rf.CommitIndex
	   	    	reply := &AppendEntriesReply{}
	   	        rf.sendAppendEntriesRPC(i, args, reply)
                      }//(i)
	   	    }(i)
	   }

} 


func (rf *Raft) LeaderState() {
      rf.BroadcastAppendEntries()
      time.Sleep(50 * time.Millisecond) // without sleep deadlock occured

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
        //fmt.Println("in candidate state function")
        rf.mu.Lock()
        rf.CurrentTerm += 1
        rf.votecount = 1
        rf.persist()
        rf.mu.Unlock()
        fmt.Println(rf.me, "current term :", rf.CurrentTerm)
	go rf.BroadcastRequestVote()
        select {
        case<- time.After(600 * time.Millisecond):
               fmt.Println("after certain time")
    	case <-rf.BecomeLeader :
                 fmt.Println(rf.me, "lala become leader")
                 rf.mu.Lock()
    		 rf.state = "leader"
                 //fmt.Println(rf.me, "lala become leader")
    		 // // When a leader first comes to power, it initializes all
			// NextIndex values to the index just after the last one in its log.
                 rf.NextIndex = make([]int, len(rf.peers))
	         rf.MatchIndex = make([]int, len(rf.peers))
		 for i := 0; i < len(rf.peers); i++ {
			rf.NextIndex[i] = len(rf.Logs)
                        // for each server, index of highest log entry
                        // known to be replicated on server
                        // (initialized to 0, increases monotonically)
                        rf.MatchIndex[i] = 0
		 }
                 rf.mu.Unlock()
           	 //go rf.BroadcastAppendEntries() 
                 //go  rf.BroadcastAppendEntries()
    	case <- rf.Heartbeat:
             fmt.Println(rf.me,"become follower")
             rf.mu.Lock()
    	     rf.state = "follower"
             rf.mu.Unlock()
        
     }
}








func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
         //fmt.Println("in make")
         rf := &Raft{}
         rf.BecomeLeader = make(chan bool, 100)
         rf.Heartbeat = make(chan bool, 100) 
         rf.Grantedvote = make(chan bool, 100)
         rf.applyCh = applyCh

         //rf.readPersist(persister.ReadRaftState())
         rf.persister = persister
         rf.state = "follower"
         rf.peers = peers
         rf.me = me
         rf.votedFor = -1
         rf.CurrentTerm = 0
         tmp := Log{}
         tmp.TERM = 0
         rf.Logs = append(rf.Logs, tmp)
         rf.CommitIndex = 0
         rf.LastApplied = 0  
         rf.readPersist(persister.ReadRaftState())
         //rf.LastApplied = 0
         /*
         Respond to RPCs from candidates and leaders
	 If election timeout elapses without receiving AppendEntries
	 RPC from current leader or granting vote to candidate:
	 convert to candidate
         */
         go func() {
          for {
            if rf.state == "follower" {
               //fmt.Println("follower:", rf.me)
               select {
              		case <-rf.Heartbeat :
                        case <-rf.Grantedvote :
              		case <-time.After(time.Duration(800 + rand.Int31n(300)) * time.Millisecond):
                      	          // can't add this because deadlock will happen
                                        //rf.mu.Lock()
                      			rf.state = "candidate"
                                        //rf.mu.Unlock()
               }

            } else if rf.state == "candidate" {
                      fmt.Println("become candidate:", rf.me)
                      rf.CandidateState()
                      fmt.Println("become candidate end:", rf.me)

            } else if rf.state == "leader" {
                      //fmt.Println("leader state:", rf.me)
                      rf.LeaderState()

            }
          }
         }()
         return rf
}

