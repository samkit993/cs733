package main
import (
	"fmt"
	"testing"
	"time"
	"os"
	"github.com/cs733-iitb/cluster/mock"
	"math/rand"
)

/* Configuration Parameters for running Tests
 * DEBUGRNTEST 														- For turning on/off debugging statements
 * TESTSIMPLE, TESTBASIC, TESTHEAVY, TESTLEADERFAILURE, TESTSPLIT 	- For running particular test function
 */
const (
	DEBUGRNTEST bool = false 
	TESTBASIC bool = true 
	TESTHEAVY bool = false 
	TESTLEADERFAILURE bool = true 
	TESTSPLIT bool = true 
)

/* Dirty debug switch
 */
func debugRaftTest(s string){
	if DEBUGRNTEST{
		fmt.Println(s)
	}
}

/* Compares expected and actual output by program
 * Both 'expect' and 'checkError' are taken from test file provided for assignment 1 by Manish(_/\_)
 */
func expect(t *testing.T, a string, b string) {
	if a != b {
		t.Error(fmt.Sprintf("Expected %v,\n found %v", a, b)) 
	}
}
func checkError(t *testing.T, err error, info string){
	if err != nil {
		t.Fatal(fmt.Sprintf("Error occurred: %s %v", err.Error(), info)) // t.Error is visible when running `go test -verbose`
	}
}

/* Creates 'nServers' Raftnodes with configuration provided in 'configFileName'
 */
func makeRafts(nServers int, configFileName string, logFileName string, hbTimeout int, timeout int) ([]*RaftNode, error){
	rafts := make([]*RaftNode, nServers)
	for id := 1; id <= nServers; id++ {
		currLogFileName := logFileName + "_" + fmt.Sprintf("%v",id) 
		os.RemoveAll(currLogFileName)
		os.RemoveAll(currLogFileName + "_state")
		node , err := NewRN(FOLLOWER, id, configFileName, currLogFileName, hbTimeout, timeout)
		if err != nil{
			fmt.Fprintln(os.Stderr,err)
			return rafts, err
		}
		rafts[id-1] = node
	}
	return rafts, nil
}

/* Creates Mock Raftnodes corresponding to Mock Cluster `mkcl` 
 */
func makeMockRafts(mkcl *mock.MockCluster,logFileName string, hbTimeout int, timeout int) ([]*RaftNode, error){
	rand.Seed(3587) 
	time.Sleep(time.Millisecond)
	rafts := make([]*RaftNode, len(mkcl.Servers))
	for id, mksrvr := range mkcl.Servers {
		currLogFileName := logFileName + "_" + fmt.Sprintf("%v",id) 
		os.RemoveAll(currLogFileName)
		os.RemoveAll(currLogFileName + "_state")
		node , err := NewMockRN(FOLLOWER, mksrvr, currLogFileName, hbTimeout, timeout)
		if err != nil{
			fmt.Fprintln(os.Stderr,err)
			return rafts, err
		}
		rafts[id-1] = node
	}
	return rafts, nil
}

/* Tests basic functionality of RaftNode.
 * Appends 2 messages to raft cluster and wait for them on commit channel
 */
func TestBasic(t *testing.T){
	if !TESTBASIC{
		return
	}
    rafts,_ := makeRafts(5, "input_spec.json", "log", 220, 300)	
	contents := make([]string, 2)
	contents[0] = "foo"
	contents[1] = "bar"
	//To get one node  elected as Leader
	time.Sleep(2*time.Second)
	rafts[0].Append([]byte(contents[0]))
	rafts[0].Append([]byte(contents[1]))
	ciarr := []int{0,0,0,0,0}
	for cnt:=0;cnt<5;{
		for idx, node := range rafts {
			select {
			case ci := <-node.CommitChannel():
				if ci.Err != nil {
					fmt.Fprintln(os.Stderr,ci.Err)
				}
				expect(t,contents[ciarr[idx]], string(ci.Data))
				ciarr[idx] += 1
				if ciarr[idx] == 2{
					cnt += 1
				}
			}
		}
	}
	for _, node := range rafts{
		//Tests LogStore actions
		node.mainLogLock.RLock()
		defer node.mainLogLock.RUnlock()
		iface, err := node.mainLog.Get(0)
		checkError(t, err,fmt.Sprintf("NodeId:%v,  mainLog.get(0) mainLog.LastIndex:%v", node.Id(), node.mainLog.GetLastIndex()))
		if iface != nil{
			foo := iface.([]byte)
			expect(t, string(foo), "foo")
			debugRaftTest(fmt.Sprintf("0:%v", string(foo)))
		}
		iface, err = node.mainLog.Get(1) 
		checkError(t, err, fmt.Sprintf("NodeId:%v,  mainLog.get(1) mainLog.LastIndex:%v", node.Id(), node.mainLog.GetLastIndex()))
		if iface != nil{
			bar := iface.([]byte)
			expect(t, string(bar), "bar")
			debugRaftTest(fmt.Sprintf("1:%v", string(bar)))
		}

		//Tests StateStore actions
		node.stateLogLock.RLock()
		defer node.stateLogLock.RUnlock()
		node.smLock.RLock()
		defer node.smLock.RUnlock()
		iface, err = node.stateLog.Get(0) 
		checkError(t, err, fmt.Sprintf("Id:%v,  stateLog.get(0)", node.Id()))
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
	time.Sleep(1*time.Second)			
}

/* Tests raft node under good enough load
 * 100 log entries are appended in one go and their response on every commit channel is tested
 * Takes long time to complete, wait for more than 30 minutes to complete. For 50 and 100, it took 122 and __ correspondigly last time I ran
 */
func TestHeavy(t *testing.T){
	if !TESTHEAVY{
		return
	}
    rafts,_ := makeRafts(5, "input_spec.json", "log", 350, 450)	
	time.Sleep(1*time.Second)
	
	rafts[0].smLock.Lock()
	ldrId := rafts[0].LeaderId() 
	rafts[0].smLock.Unlock()
	debugRaftTest(fmt.Sprintf("Leader Id:%v\n", ldrId))
	contents := make([]string, 120)
	for i := 1; i<=100;i++{
		contents[i-1] = fmt.Sprintf("word%d", i)
		rafts[0].Append([]byte(contents[i-1]))
	}
	ciarr := []int{0,0,0,0,0}
	for cnt:=0;cnt<5;{
		for idx, node := range rafts {
			select {
			case ci := <-node.CommitChannel():
				if ci.Err != nil {
					fmt.Fprintln(os.Stderr,ci.Err)
				}
				expect(t,contents[ciarr[idx]], string(ci.Data))
				ciarr[idx] += 1
				if ciarr[idx] == 100{
					cnt += 1
				}
				debugRaftTest(fmt.Sprintf("####### NodeId(%v) CommitIndex(%v) OverallCount(%v)\n", idx+1, ciarr[idx], cnt))
			}
		}
	}
	for _, node := range rafts {
		node.smLock.RLock()
		fmt.Printf("%v", node.sm.String())
		node.smLock.RUnlock()
		node.Shutdown()
	}
	time.Sleep(1*time.Second)
}

/* Test fault tolerance of cluster when leader fails 
 * Starts with 5 raftnodes. After processing one message, leader fails
 * In resultant cluster with 4 nodes, new log entry is appended and their response on commit channel  checked
 */
func TestLeaderFailure(t *testing.T){
	if !TESTLEADERFAILURE{
		return
	}
    rafts,_ := makeRafts(5, "input_spec.json", "log", 200, 300)	
	time.Sleep(2*time.Second)			
	running := make(map[int]bool)
	for i:=1;i<=5;i++{
		running[i] = true
	}
	rafts[0].smLock.RLock()
	lid := rafts[0].LeaderId()
	rafts[0].smLock.RUnlock()
	debugRaftTest(fmt.Sprintf("leader Id:%v\n", lid))
	if lid != -1{
		rafts[lid-1].Shutdown()
		debugRaftTest(fmt.Sprintf("Leader(id:%v) is down, now\n", lid))
		time.Sleep(4*time.Second)			
		running[lid]  = false
		for i := 1; i<= 5;i++{
			if running[i] {
				rafts[i-1].Append([]byte("first"))
				break
			}
		}
	}
	time.Sleep(5*time.Second)			

	for idx, node := range rafts {
		node.smLock.RLock()
		debugRaftTest(fmt.Sprintf("%v", node.sm.String()))
		node.smLock.RUnlock()
		if running[idx-1]{
			node.Shutdown()
		}
	}
}

/* Tests fault tolerance of cluster in the event of network partition 
 * Splits cluster such that partition with leader stays in majority
 * In paritioned network, logentry is appended to leader's log.
 * Proper replication of log is tested once partition is healed 
 */
func TestSplit(t *testing.T){
	if !TESTSPLIT{
		return
	}
	contents := make([]string, 2)
	contents[0] = "duckduck"
	contents[1] = "go"
	mkcl, err := mock.NewCluster("input_spec.json")
	rafts,err := makeMockRafts(mkcl,"log", 250, 350) 
	checkError(t,err, "While creating mock clusters")
	time.Sleep(5*time.Second)
	rafts[0].Append([]byte(contents[0]))
	time.Sleep(5*time.Second)
	mkcl.Lock()
	part1 := []int{1,3}
	part2 := []int{2,4}
	rafts[1].smLock.RLock()
	ldrId := rafts[4].LeaderId()
	rafts[1].smLock.RUnlock()
	fmt.Printf("ldrId:%v\n", ldrId)
	if ldrId % 2 == 0{
		part2 = append(part2, 5)
	}else{
		part1 = append(part1, 5)
	}
	mkcl.Unlock()
	mkcl.Partition(part1, part2)
	debugRaftTest(fmt.Sprintf("Partitions: %v %v\n", part1, part2))
	time.Sleep(4*time.Second)
	mkcl.Lock()
	rafts[ldrId-1].Append([]byte(contents[1]))
	mkcl.Unlock()
	time.Sleep(8*time.Second)
	mkcl.Heal()
	debugRaftTest(fmt.Sprintf("Healed\n"))
	time.Sleep(8*time.Second)
	ciarr := []int{0,0,0,0,0}
	for cnt:=0;cnt<5;{
		for idx, node := range rafts {
			select {
			case ci := <-node.CommitChannel():
				if ci.Err != nil {
					fmt.Fprintln(os.Stderr,ci.Err)
				}
				//Testing CommitChannel 
				expect(t,contents[ciarr[idx]],string(ci.Data))
				ciarr[idx] += 1
				if ciarr[idx] == 2{
					cnt +=1 
				}
			}
		}
	}

	for _, node := range rafts {
		node.smLock.RLock()
		debugRaftTest(fmt.Sprintf("%v", node.sm.String()))
		node.smLock.RUnlock()
		node.Shutdown()
	}
}
