package main
import (
	"fmt"
	"testing"
	"time"
	"os"
	"encoding/gob"
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/cluster/mock"
	"math/rand"
)
var Debug bool =  false 
func debug(s string){		//Dirty debug switch
	if Debug{
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

func makeRafts(nServers int, configFileName string, logFileName string, hbTimeout int, timeout int) ([]*RaftNode, error){
	rafts := make([]*RaftNode, nServers)
	for id := 1; id <= nServers; id++ {
		node , err := NewRN(FOLLOWER, id, configFileName, fmt.Sprintf("%v%d", logFileName, id), hbTimeout, timeout)
		if err != nil{
			fmt.Fprintln(os.Stderr,err)
			return rafts, err
		}
		rafts[id-1] = node
	}
	return rafts, nil
}

func makeMockRafts(mkcl *mock.MockCluster,logFileName string, hbTimeout int, timeout int) ([]*RaftNode, error){
	rand.Seed(3007) 
	time.Sleep(time.Millisecond)
	rafts := make([]*RaftNode, len(mkcl.Servers))
	mkcl.Lock()
	defer mkcl.Unlock()
	for id, mksrvr := range mkcl.Servers {
		node , err := NewMockRN(FOLLOWER, mksrvr, fmt.Sprintf("%v%d", logFileName, id+1), hbTimeout, timeout)
		if err != nil{
			fmt.Fprintln(os.Stderr,err)
			return rafts, err
		}
		rafts[id-1] = node
	}
	return rafts, nil
}

//Following function tests basic functionality of Raft Implementation
func TestBasic(t *testing.T){
	registerStructs()
    rafts,_ := makeRafts(5, "input_spec.json", "log", 150, 300)	
	//To get one node  elected as Leader
	time.Sleep(1*time.Second)			
	rafts[0].Append([]byte("foo"))
	time.Sleep(1*time.Second)			
	rafts[0].Append([]byte("bar"))
	//To get commit actions executed
	time.Sleep(3*time.Second)			
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
		debug(fmt.Sprintf("%v\n, mainLog.LastIndex(%v) stateLog.LastIndex(%v)\n", node.sm.String(), node.mainLog.GetLastIndex(), node.stateLog.GetLastIndex()))
	
		/*
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
		*/
	}

	for _, node := range rafts {
		node.Shutdown()
	}
}
func TestLeaderFailure(t *testing.T){
    rafts,_ := makeRafts(5, "input_spec.json", "log", 150, 300)	
    running := make(map[int]bool)	
	for i := 1; i <= 5; i++{
		running[i] = true
	}
	time.Sleep(2*time.Second)			
	lid := rafts[0].LeaderId()
	debug(fmt.Sprintf("^^^^^^^^^^^^^^^^^^^^^^leader Id:%v\n", lid))
	if lid != -1{
		rafts[lid-1].Shutdown()
		debug(fmt.Sprintf("=====================Leader(id:%v) is down, now\n", lid))
		time.Sleep(4*time.Second)			
		running[lid]  = false
		for i := 1; i<= 5;i++{
			if running[i] {
				rafts[i-1].Append([]byte("first"))
				break
			}
		}
	}
	time.Sleep(3*time.Second)			
	for i, node := range rafts{
		if running[i+1] != true {
			continue
		}
		select{
			case ci := <-node.CommitChannel():
				if ci.Err != nil {
					fmt.Fprintln(os.Stderr,ci.Err)
				}
				//Testing CommitChannel 
				expect(t,string(ci.Data), "first") 						
			default:
				t.Fatal("Expected message on all nodes")
		}
	}
}
/*
func TestSplit(t *testing.T){
	mkcl, err := mock.NewCluster("input_spec.json")
	rafts,err := makeMockRafts(mkcl,"log", 150, 350) 
	checkError(t,err)
	time.Sleep(2*time.Second)
	rafts[0].Append([]byte("duckduck"))
	time.Sleep(2*time.Second)
	mkcl.Lock()
	part1 := []int{1,3}
	part2 := []int{2,4}
	ldrId := rafts[4].LeaderId()
	if ldrId % 2 == 0{
		part2 = append(part2, 5)
	}else{
		part1 = append(part1, 5)
	}
	mkcl.Unlock()
	mkcl.Partition(part1, part2)
	debug(fmt.Sprintf("Partitions: %v %v\n", part1, part2))
	time.Sleep(2*time.Second)
	mkcl.Lock()
	rafts[ldrId-1].Append([]byte("go"))
	mkcl.Unlock()
	time.Sleep(3*time.Second)
	mkcl.Heal()
	debug(fmt.Sprintf("Healed\n"))
	time.Sleep(4*time.Second)
	for _, node := range rafts {
		select {
		case ci := <-node.CommitChannel():
			if ci.Err != nil {
				fmt.Fprintln(os.Stderr,ci.Err)
			}
			//Testing CommitChannel 
			expect(t,"duckduck",string(ci.Data)) 						
		}
		expect(t, "2", fmt.Sprintf("%v",node.sm.commitIndex))		//StateMachine's update of commitIndex tested
		fmt.Println(node.sm.String())
	}
}
*/
