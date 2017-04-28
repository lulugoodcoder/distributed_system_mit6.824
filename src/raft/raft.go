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
import "strconv"

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
	//println("rf.me " + strconv.Itoa(rf.me) + " : In readPersist, len(rf.logs): " + strconv.Itoa(len(rf.Logs)) + " Term: " + strconv.Itoa(rf.CurrentTerm) + " CommitIndex: " + strconv.Itoa(rf.CommitIndex))
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

	fmt.Println("%v", rf.me)
	fmt.Println(":request vote\n")
	if args.TERM < rf.currentTerm {
		 reply.VOTEGRANTED = false
		 reply.TERM = rf.currentTerm
		 return
	}
        if rf.currentTerm < args.TERM {
		rf.mu.Lock()
		rf.state = "follower"
		rf.currentTerm = args.TERM
		rf.mu.Unlock()
	}

     uptodate := Uptodate(rf.logs[len(rf.logs) - 1].TERM , len(rf.logs) - 1, args.LASTLOGTERM, args.LASTLOGIDX)
     if uptodate == true && rf.votedFor != args.CANDIDATEID {
	rf.votedFor = args.CANDIDATEID
	reply.VOTEGRANTED = true
	rf.currentTerm = args.TERM
	reply.TERM = args.TERM
	rf.state = "follower"
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
	   fmt.Println("%v", rf.me)
	   fmt.Println(":send request vote\n")
	   rf.mu.Lock()
	   rf.votecount = 1
	   rf.mu.Unlock()
	   ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	   if ok {
	         if reply.TERM > rf.currentTerm {
	               rf.mu.Lock()
	               rf.state = "follower"
                       rf.currentTerm = reply.TERM
		       rf.mu.Unlock()
               } else if reply.VOTEGRANTED == true {
               	  rf.mu.Lock()
               	  rf.votecount = rf.votecount + 1
                  fmt.Println("rf me", rf.me)
                  fmt.Println("rf votecount", rf.votecount)
		  fmt.Println("rf half len", len(rf.peers) / 2)
               	  if rf.votecount > len(rf.peers) / 2 && rf.state == "candidate" {
                      rf.BecomeLeader <- true
               	  }               	  
               	   rf.mu.Unlock()
               } 
	   }
	   return ok
}

// send RequestVote to all other nodes in the cluster
func (rf *Raft) BroadcastRequestVote() {
	  fmt.Println("%v", rf.me)
	  fmt.Println(":broadcast request vote\n")		
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
	 ENTRIES []Log
	 LEADERCOMMIT int
}

type AppendEntriesReply struct {
	TERM int
	SUCCESS bool
	NEXTINDEX int
}

func (rf *Raft) AppendEntriesRPC(args *AppendEntries, reply *AppendEntriesReply) {
	            fmt.Println(strconv.Itoa(rf.me) + ":append entries\n")
	/*rf.mu.Lock()
	defer rf.mu.Unlock()
        if rf.currentTerm > args.TERM {
                reply.TERM = rf.currentTerm
		reply.SUCCESS= false
		reply.NEXTINDEX = len(rf.logs) - 1
		return
	}
        rf.Heartbeat <- true
        if rf.state == "candidate" {
		rf.state = "follower"
	}
	if rf.currentTerm < args.TERM {
		rf.currentTerm = args.TERM
	}
       //  Append any new entries not already in the log
	  
        if len(rf.logs) > args.PREVLOGINDEX && rf.logs[args.PREVLOGINDEX].TERM == args.PREVLOGTERM {
		reply.SUCCESS = true
		reply.TERM = rf.currentTerm
                        rf.logs = rf.logs[:args.PREVLOGINDEX+1] // include value at index: args.PREVLOGINDEX
			for i := 0; i < len(args.ENTRIES); i++ {
				rf.logs = append(rf.logs, args.ENTRIES[i])
			}
                 if rf.commitIndex < args.LEADERCOMMIT {
			if args.LEADERCOMMIT < len(rf.logs)-1 {
				rf.commitIndex = args.LEADERCOMMIT
			} else {
				rf.commitIndex = len(rf.logs) - 1
			}
		}
                //If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		reply.NEXTINDEX = len(rf.logs) - 1
		//go rf.persist()
	} else {
	       if args.PREVLOGINDEX - 1 <= rf.commitIndex {
			reply.NEXTINDEX = args.PREVLOGINDEX - 1
		} else {
			reply.NEXTINDEX = rf.commitIndex
		}
		
		reply.SUCCESS = false
	}*/
	            if args.TERM < rf.currentTerm {
	            	reply.SUCCESS = false
	            	reply.TERM = rf.currentTerm
	            	//  // If the follower has all the entries the leader sent, 
	            	//the follower MUST NOT truncate its log.
	            	reply.NEXTINDEX = len(rf.logs) - 1 
	            	return
	            }
	            if rf.currentTerm < args.TERM {
	            	rf.mu.Lock()
	            	rf.currentTerm = args.TERM
	            	rf.state = "follower"
	            	rf.mu.Unlock()
	            }
                /*if len(rf.logs) <= args.PREVLOGINDEX {
                    reply.SUCCESS = false
	            	reply.TERM = args.TERM
	            	reply.NEXTINDEX = len(rf.logs) - 1
	            	return
                }*/
                rf.Heartbeat <- true 
                /*if  len(rf.logs) > args.PREVLOGINDEX && rf.logs[args.PREVLOGINDEX].TERM != args.TERM {
                	reply.SUCCESS = false
                	reply.TERM = args.TERM
                	reply.NEXTINDEX = args.PREVLOGINDEX
                }*/
             /*  if rf.commitIndex < args.LEADERCOMMIT {
					if args.LEADERCOMMIT < len(rf.logs)-1 {
						rf.commitIndex = args.LEADERCOMMIT
					} else {
						rf.commitIndex = len(rf.logs) - 1
					}
				}*/
				fmt.Println("log length :%v", len(rf.logs))
				fmt.Println("prevlog: %v", args.PREVLOGINDEX)
				//fmt.Println("prevlog term: %v", rf.logs[args.PREVLOGINDEX].TERM)
				fmt.Println("args term: %v", args.TERM)
               if len(rf.logs) > args.PREVLOGINDEX && rf.logs[args.PREVLOGINDEX].TERM == args.PREVLOGTERM {
                	rf.mu.Lock()
                	rf.logs = rf.logs[0 : args.PREVLOGINDEX + 1]
                        fmt.Println("args.entries" + strconv.Itoa(len(args.ENTRIES)))
                	for  i := 0; i < len(args.ENTRIES); i++ {
                		rf.logs = append(rf.logs, args.ENTRIES[i])
                		fmt.Println(":append to %v" , len(rf.logs) - 1)
                                
                		if str, ok := args.ENTRIES[i].COMMAND.(int); ok {
    				     fmt.Println(":content" + strconv.Itoa(str))
				} 

                	}
                	rf.mu.Unlock()
                	reply.SUCCESS = true
                	reply.TERM = args.TERM
                	reply.NEXTINDEX = len(rf.logs) - 1
                	if args.LEADERCOMMIT > rf.commitIndex {
                		 if args.LEADERCOMMIT < len(rf.logs) - 1 {
                		 	rf.mu.Lock()
                		 	rf.commitIndex = args.LEADERCOMMIT
                		 	rf.mu.Unlock()
                		 } else {
                		 	rf.mu.Lock()
                		 	rf.commitIndex = len(rf.logs) - 1
                		 	rf.mu.Unlock()
                		 }
                	}
                        fmt.Println("inside append", rf.commitIndex)
                        fmt.Println("inside append" + strconv.Itoa(len(rf.logs))) 
                	go rf.persist()

                } else {
                	 reply.SUCCESS = false
                	 reply.TERM = args.TERM
                	 // easily wrong part
                	 fmt.Println("not ok")
			         if args.PREVLOGINDEX - 1 <= rf.commitIndex {
						reply.NEXTINDEX  = args.PREVLOGINDEX - 1
					 } else {
						reply.NEXTINDEX = rf.commitIndex
					 }
				}               
}

func (rf *Raft) sendAppendEntriesRPC(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	// Calling peer node indefinitely until the other side has response (for loop).
	// As long as the other side has response, check if it has accepted as a leader,
	// if not, check if leader's term is up-to-date, if not, step down to follower
	     fmt.Println("%v", rf.me)
	     fmt.Println(":send append entries\n")	
         ok := rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)
         if ok {
        	 if reply.TERM > rf.currentTerm {
        	 	rf.mu.Lock()
        	 	rf.state = "follower"
        	 	rf.currentTerm = reply.TERM
         		rf.mu.Unlock()
                        //fmt.Println("be follower:", rf.me)
       		 }
       		 rf.mu.Lock()
       		 rf.nextIndex[server] = reply.NEXTINDEX
       		 rf.mu.Unlock()
       		  fmt.Println("replyindex%v", rf.nextIndex[server])
         } else {
        	 rf.nextIndex[server] = 0
         }
		return ok
}

// send AppendEntriesRPC to all other nodes in the cluster
func (rf *Raft) BroadcastAppendEntriesRPC() {
	   fmt.Println("%v", rf.me)
	   fmt.Println(":broadcast append entries\n")
	   for i := 0; i < len(rf.peers); i++ {
	   	    if i != rf.me && rf.state == "leader" {
                    go func(i int) {
	   	    	args := &AppendEntries{}
	   	    	args.TERM = rf.currentTerm
	   	    	args.LEADERID = rf.me
	   	    	args.PREVLOGINDEX =  rf.nextIndex[i]// easily wrong
	   	    	fmt.Println("next index : %v", args.PREVLOGINDEX)
	   	    	args.PREVLOGTERM = rf.logs[args.PREVLOGINDEX].TERM // easily wrong
	   	    	args.ENTRIES = rf.logs[args.PREVLOGINDEX + 1 :] // easily wron
                        args.LEADERCOMMIT = rf.commitIndex
	   	    	reply := &AppendEntriesReply{}
	   	    	//
	   	        rf.sendAppendEntriesRPC(i, args, reply)
                      }(i)
	   	    }
	   }
	   go rf.persist()         
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
	             fmt.Println("start", rf.me)
                 index := len(rf.logs) - 1
                 term := rf.currentTerm
                 isLeader := false         
                 if rf.state == "leader" {
                    l := Log{}
                    l.TERM = rf.currentTerm
                    l.COMMAND = command
                    rf.mu.Lock()
                    rf.logs = append(rf.logs,l) 
                    rf.nextIndex[rf.me] = len(rf.logs) - 1
                    index = len(rf.logs) - 1
                    fmt.Println("in start content", command)
                    fmt.Println("in start next index", rf.nextIndex[rf.me])
                    rf.mu.Unlock()
                    isLeader = true
                     for i := 0; i < len(rf.peers); i++ {
                               if (i != rf.me) {
                                  go func(i int) {
                                     rf.mu.Lock()
	   	                     args := &AppendEntries{}
	   	    	             args.TERM = rf.currentTerm
	   	    	             args.LEADERID = rf.me
	   	    	             args.PREVLOGINDEX= rf.nextIndex[i] // easily wrong
	   	    	             args.ENTRIES = rf.logs[args.PREVLOGINDEX + 1 :] // easily wrong
	   	    	             args.PREVLOGTERM = rf.logs[args.PREVLOGINDEX].TERM // easily wrong
	   	    	             reply := &AppendEntriesReply{}
                                      rf.mu.Unlock()
                                      rf.sendAppendEntriesRPC(i, args, reply)
                                  }(i)
                              }
                     }
                    
                }
               fmt.Println("end")
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
	fmt.Println("%v", rf.me)
	fmt.Println(":candidatestate\n")
    rf.currentTerm += 1
	go rf.BroadcastRequestVote()
    select {
    	case <-rf.BecomeLeader :
    		 rf.mu.Lock()
    		 rf.state = "leader"
    		 // // When a leader first comes to power, it initializes all
			// NextIndex values to the index just after the last one in its log.
    		 rf.nextIndex=  []int{}
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex = append(rf.nextIndex, len(rf.logs) - 1)
				}
		rf.mu.Unlock()		
		//		go rf.BroadcastAppendEntriesRPC()
    		 
    	case <- rf.Heartbeat:
    	     rf.mu.Lock()
    	     rf.state = "follower"
    	     rf.mu.Unlock()
        case<- time.After(600 * time.Millisecond):
        	 rf.mu.Lock()
        	 rf.state = "follower"
        	 rf.mu.Unlock()
    }
	
                
		// candidate state continues until a) it wins the
		// election, (b) another server establishes itself as leader, or
		// (c) a period of time goes by with no winner
	
}

func (rf *Raft) LeaderState() {
	fmt.Println("%v", rf.me)
	fmt.Println(":leaderstate\n")
	time.Sleep(10 * time.Millisecond)
        if (rf.commitIndex == rf.lastApplied) {
            time.Sleep(25 * time.Millisecond)
        }
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
         rf.persister = persister
         rf.readPersist(persister.ReadRaftState())
         go func() {
         	for {
         		//fmt.Printf("ingo")
         		if rf.state == "follower" {
         			fmt.Println("follower")
            		select {
              			case <-rf.Heartbeat :
	           		fmt.Println("")
              			fmt.Println("%v", rf.me)
	                        fmt.Println(":do follower\n")
              			case <-time.After(time.Duration(800+ rand.Int31n(300)) * time.Millisecond):
                      	rf.mu.Lock()
                      	fmt.Println("candidate")
                      	rf.state = "candidate"
                      	rf.mu.Unlock()
            		}
            	} else if rf.state == "candidate" {
            		    fmt.Println("%v", rf.me)
	                    fmt.Println(":do candidate\n")
              			rf.CandidateState()
         		} else if rf.state == "leader" {
         			    fmt.Println("%v", rf.me)
	                    fmt.Println(":do leader\n")
              			rf.LeaderState()
         		}
         	}
         }()
         // first we need to get the commitindex of the leader
         // The leader decides when it is safe to apply a log entry
		//to the state machines; such an entry is called committed.
		//Raft guarantees that committed entries are durable
		//and will eventually be executed by all of the available
		//state machines. A log entry is committed once the leader
		//that created the entry has replicated it on a majority of
		//the servers
        
      go func() {
        	for { 
        		 //fmt.Printf("outgo")
        		 time.Sleep(20 * time.Millisecond)                      
         		 if rf.state == "leader" {
                                rf.mu.Lock()
         		 	count := 0
         		    commitindex := rf.nextIndex[rf.me]
                            fmt.Println("in go func : commitindex" ,rf.commitIndex)                           
         		    // we need to update the commitindex 
         		    // so if the nextindex is greater than commitindex, we get a chance to update
         		    // we find the smallest nextindex 
         		    for i := 0; i < len(rf.peers); i++ {
                        fmt.Println("in go func:next index", rf.nextIndex[i])
                        if rf.nextIndex[i] > rf.commitIndex {
                        	count++
                            if  commitindex > rf.nextIndex[i] {
                            	commitindex = rf.nextIndex[i]
                                fmt.Println("in go func: commitindex", commitindex)
                            }
                            
                        }
                        fmt.Println("in go func:commitindex", commitindex)
			fmt.Println("in go func:count", count)
         		    }
         		   
         		    if rf.state == "leader" && count > len(rf.peers) / 2 {
				fmt.Println("in go func:is leader")   		    	
         		    	rf.commitIndex = commitindex
         		    }
                            rf.mu.Unlock()
         		    fmt.Println("in go func : commitindex" ,rf.commitIndex)
         		    go rf.persist()
         		}
         		    // after update the commit index of the leader, we need to send via applymsg
         		//fmt.Printf("mid")
			fmt.Println("last applied and comitindex")
                        fmt.Println(rf.lastApplied)
			fmt.Println(rf.commitIndex)
                        
         		if (rf.lastApplied < rf.commitIndex) {
				fmt.Println("in about ")
                                fmt.Println("me :")
                                fmt.Println(rf.me)
                                fmt.Println("rf term", rf.currentTerm)
                                fmt.Println("commit index")
         			fmt.Println( rf.commitIndex)
				fmt.Println("last applied")
				fmt.Println(rf.lastApplied)
         			go func(){
                                rf.mu.Lock()
				lastApplied := rf.lastApplied + 1
                                oldcommitIndex := rf.commitIndex 
                                rf.lastApplied = rf.commitIndex
                                rf.mu.Unlock() 
                                fmt.Println("son commit index", oldcommitIndex)
				fmt.Println("son last applied", lastApplied)  
                              //  time.Sleep(10 * time.Millisecond)
                    	for i := lastApplied; i <= oldcommitIndex; i++ {
							fmt.Println("last applied")
							fmt.Println("begin to send")
                    					msg := ApplyMsg{}
                    					msg.Index = i
							msg.Command = rf.logs[i].COMMAND
							applyCh <- msg
						}
                                  
					}()
                                        
					
				} 
                                
                                    
         	}
         }()
         
         return rf
} 
