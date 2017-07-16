package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
        "time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
        Kind string // append, get ...
        Key string
        Value string
        ClientId int64
        RequestId int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
         
	maxraftstate int // snapshot if log grows this big
   
	// Your definitions here.
        data map[string]string           //data key value pairs
        lastex map[int64]int            // current server the last id it executed
        currentop map[int]chan Op      // current server and the op it is executing
}

func(kv *RaftKV) loop() {
   for {
     applychan := <- kv.applyCh
     kv.mu.Lock()
     //https://tour.golang.org/methods/15
     op := applychan.Command.(Op)
     if kv.lastex[op.ClientId] < op.RequestId {
        switch kind := op.Kind; kind {
           case "Put" :
                 log.Printf("Put in loop")
                 kv.data[op.Key] = op.Value
           case "Append" :
                 if _, ok := kv.data[op.Key]; ok {
                       kv.data[op.Key] += op.Value
                 } else {
                       kv.data[op.Key] = op.Value
                 }
                 log.Printf("append", op.Key, kv.data[op.Key])
           case "Get":
        }
        kv.lastex[op.ClientId] = op.RequestId
     } 
     go func(apply raft.ApplyMsg) {
             kv.mu.Lock()
             applied, ok := kv.currentop[apply.Index]
             kv.mu.Unlock()
             if  ok {
                 applied <- apply.Command.(Op)
             }
     }(applychan)
     kv.mu.Unlock()
   }
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
       // Your code here.
       var op Op
       op.Kind = "Get"
       op.Key = args.Key
       op.ClientId = args.ClientId 
       op.RequestId = args.RequestId
       ok := kv.AppendtoLog(op)
       if ok {
          reply.WrongLeader = false 
          kv.mu.Lock()
          defer kv.mu.Unlock()      
	  if val, ok := kv.data[args.Key]; ok {
             reply.Err =  OK
             reply.Value = val
          } else {
             reply.Err = ErrNoKey
          }  
       } else {
         reply.WrongLeader = true
       }      
}

func (kv* RaftKV) AppendtoLog(op Op) bool {
      index, _, isLeader := kv.rf.Start(op)
      if !isLeader {
         return false
      }
      kv.mu.Lock()
      appliedChan, ok := kv.currentop[index]
      if !ok {
             appliedChan = make(chan Op)
             kv.currentop[index] = appliedChan
      }
      kv.mu.Unlock()
      select {
           case c := <- appliedChan:
                 return op == c
           case <- time.After(200 * time.Millisecond):
                 return false
      }

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
       var op Op
       op.Kind = args.Op
       op.Key = args.Key
       op.Value = args.Value
       op.ClientId = args.ClientId 
       op.RequestId = args.RequestId
       ok := kv.AppendtoLog(op)
       if (ok) {
          reply.WrongLeader = false
       } else {
          reply.WrongLeader = true
       }       
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
        
	// You may need initialization code here.
        kv.data = make(map[string]string)           //data key value pairs
        kv.lastex = make(map[int64]int)           // current server the last id it executed
        kv.currentop = make(map[int]chan Op)  

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
        go kv.loop()
	return kv
}
