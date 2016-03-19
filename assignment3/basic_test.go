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

func TestBasic(t *testing.T){
	rand.Seed(10000007) 
	registerStructs()
	raft1, err := NewRaftNode(FOLLOWER, 1, "input_spec.json", "log1", 150, 250)
	checkError(t,err)
	raft2, err := NewRaftNode(FOLLOWER, 2, "input_spec.json", "log2", 150, 250)
	checkError(t,err)
	raft3, err := NewRaftNode(FOLLOWER, 3, "input_spec.json", "log3", 150, 250)
	checkError(t,err)
    rafts := make([]*RaftNode, 3)	
	rafts[0] = raft1
	rafts[1] = raft2
	rafts[2] = raft3

	time.Sleep(1*time.Second)
	raft2.Append([]byte("foo"))
	raft2.Append([]byte("bar"))
	time.Sleep(1*time.Second)
	for _, node := range rafts {
		select {
		case ci := <-node.CommitChannel():
			if ci.Err != nil {
				fmt.Fprintln(os.Stderr,ci.Err)
			}
			expect(t,string(ci.Data), "foo") 
		}
		expect(t, fmt.Sprintf("%v",node.sm.commitIndex), "2")
	}
}
