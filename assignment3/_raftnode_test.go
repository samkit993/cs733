package main
import (
	"fmt"
	"testing"
	"time"
	"os"
	"encoding/gob"
	"github.com/cs733-iitb/cluster"
	"math/rand"
)

func debug(s string){		//Dirty debug switch
	if false{
		fmt.Println(s)
	}
}

func checkError(t *testing.T, err error){
	if err != nil {
		t.Error(fmt.Sprintf("Error occurred: %s", err.Error())) // t.Error is visible when running `go test -verbose`
	}
}

func expect(t *testing.T, a string, b string) {
	if a != b {
		t.Error(fmt.Sprintf("Expected %v,\n found %v", a, b)) // t.Error is visible when running `go test -verbose`
	}
}

func registerStructs(){
	gob.Register(cluster.Envelope{})
	gob.Register(Send{})
	gob.Register(Message{})
	gob.Register(AppendEv{})
	gob.Register(AppendEntriesReqEv{})
	gob.Register(AppendEntriesRespEv{})
	gob.Register(VoteReqEv{})
	gob.Register(VoteRespEv{})
}

//Following function tests basic functionality of Raft Implementation
func TestBasic(t *testing.T){
	rand.Seed(10000007) 
	registerStructs()
	raft1, err := NewRaftNode(FOLLOWER, 1, "input_spec.json", "log1", 150, 250)
	checkError(t,err)
	raft2, err := NewRaftNode(FOLLOWER, 2, "input_spec.json", "log2", 150, 250)
	checkError(t,err)
	raft3, err := NewRaftNode(FOLLOWER, 3, "input_spec.json", "log3", 150, 250)
	checkError(t,err)
	raft4, err := NewRaftNode(FOLLOWER, 4, "input_spec.json", "log4", 150, 250)
	checkError(t,err)
	raft5, err := NewRaftNode(FOLLOWER, 5, "input_spec.json", "log5", 150, 250)
	checkError(t,err)
	raft6, err := NewRaftNode(FOLLOWER, 6, "input_spec.json", "log6", 150, 250)
	checkError(t,err)
    rafts := make([]*RaftNode, 6)	
	rafts[0] = raft1
	rafts[1] = raft2
	rafts[2] = raft3
	rafts[3] = raft4
	rafts[4] = raft5
	rafts[5] = raft6
	return 
	//To get one node  elected as Leader
	time.Sleep(1*time.Second)			
	raft2.Append([]byte("foo"))
	raft2.Append([]byte("bar"))
	//To get commit actions executed
	time.Sleep(1*time.Second)			
	for _, node := range rafts {
		select {
		case ci := <-node.CommitChannel():
			if ci.Err != nil {
				fmt.Fprintln(os.Stderr,ci.Err)
			}
			//Testing CommitChannel 
			expect(t,string(ci.Data), "foo") 						
		}
		expect(t, fmt.Sprintf("%v",node.sm.commitIndex), "2")		//StateMachine's update of commitIndex tested
		fmt.Printf("%v\n, mainLog.LastIndex(%v) stateLog.LastIndex(%v)\n", node.sm.String(), node.mainLog.GetLastIndex(), node.stateLog.GetLastIndex())
		
		//Testing LogStore 
		iface, err := node.mainLog.Get(0) 
		checkError(t, err)
		if iface != nil{
			foo := iface.([]byte)
			expect(t, string(foo), "foo") 							
			debug(fmt.Sprintf("0:%v", string(foo)))
		}
		iface, err = node.mainLog.Get(1) 
		if iface != nil{
			bar := iface.([]byte)
			checkError(t, err)
			expect(t, string(bar), "bar")
			debug(fmt.Sprintf("1:%v", string(bar)))
		}

		//Testing StateStore
		iface, err = node.stateLog.Get(0) 
		checkError(t, err)
		if iface != nil{
			state := iface.(StateInfo)
			expect(t, fmt.Sprintf("%v", state.CurrTerm), fmt.Sprintf("%v", node.sm.currTerm))
			expect(t, fmt.Sprintf("%v", state.VotedFor), fmt.Sprintf("%v", node.sm.votedFor))
			expect(t, state.Log.String(), node.sm.log.String())
		}
	}

	for _, node := range rafts {
		node.Shutdown()
	}
}

func TestSplit(t *testing.T){
}
