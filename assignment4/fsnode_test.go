package main
import (
	"fmt"
	"testing"
	"time"
	"os"
	"bufio"
	"sync"
	"runtime/debug"
	"strings"
	"strconv"
	"net"
	"github.com/cs733-iitb/cluster/mock"
	"math/rand"
)

const (
	DEBUGFSNTEST = false 
	TESTBASIC = false 
	TESTSEQUENTIAL = false 
	TESTCONCURRENT = true 
)
/* Dirty debug switch
 */
func debugFsnTest(s string){
	if DEBUGFSNTEST{
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
func makeFsNodes(nServers int, configFileName string, logFileName string, hbTimeout int, timeout int, pubaddrs []string) ([]*FsNode, error){
	//TODO Recovery
	rand.Seed(3587) 
	fsnodes := make([]*FsNode, nServers)
	for id := 1; id <= nServers; id++ {
		currLogFileName := logFileName + "_" + fmt.Sprintf("%v",id) 
		//os.RemoveAll(currLogFileName)
		//os.RemoveAll(currLogFileName + "_state")
		node , err := NewFsNode(FOLLOWER, id, configFileName, currLogFileName, hbTimeout, timeout, pubaddrs[id-1])
		if err != nil{
			fmt.Fprintln(os.Stderr,err)
			return fsnodes, err
		}
		fsnodes[id-1] = node
	}
	return fsnodes, nil
}

/* Creates Mock Raftnodes corresponding to Mock Cluster `mkcl` 
 */
func makeMockFsNodes(mkcl *mock.MockCluster,logFileName string, hbTimeout int, timeout int, pubaddrs []string) ([]*FsNode, error){
	rand.Seed(3587) 
	time.Sleep(time.Millisecond)
	fsnodes := make([]*FsNode, len(mkcl.Servers))
	for id, mksrvr := range mkcl.Servers {
		currLogFileName := logFileName + "_" + fmt.Sprintf("%v",id) 
		//os.RemoveAll(currLogFileName)
		//os.RemoveAll(currLogFileName + "_state")
		node , err := NewMockFsNode(FOLLOWER, mksrvr, currLogFileName, hbTimeout, timeout, pubaddrs[id-1])
		if err != nil{
			fmt.Fprintln(os.Stderr,err)
			return fsnodes, err
		}
		fsnodes[id-1] = node
	}
	return fsnodes, nil
}

func GetConnection(t *testing.T, addr string) net.Conn{
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err.Error()) 
	}
	return conn
}

func  Write(conn net.Conn, filename string, contents string, exptime int) int64 {
    reader := bufio.NewReader(conn)
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", filename, len(contents), exptime, contents)
    resp, ok := reader.ReadBytes('\n')
	debugFsnTest(fmt.Sprintf("Write got response:%v\n", string(resp)))
	if ok != nil{
		fmt.Fprintf(os.Stderr, "Error in reading from socket in Write\n")
		fmt.Printf("Error in reading from socket in Write\n")
		debug.PrintStack()
		os.Exit(2)
	}
	arr := strings.Fields(string(resp)) 
	if arr[0] != "OK"{
		fmt.Fprintf(os.Stderr, "Expected: %v found: %v in Write 1\n","OK", arr[0])
		fmt.Printf("Expected: %v found: %v in Write 1\n","OK", arr[0])
		debug.PrintStack()
		os.Exit(2)
	}
	version, err := strconv.ParseInt(arr[1], 10, 64) 
	if err != nil {
		fmt.Fprintf(os.Stderr, "Non-numeric version found in Write")
		fmt.Printf("Non-numeric version found in Write")
		debug.PrintStack()
		os.Exit(2)
	}
	return version 
}

func  ReadSuccess(conn net.Conn, filename string, contents string, version int64){
    reader := bufio.NewReader(conn)
	fmt.Fprintf(conn, "read %v\r\n", filename)
	resp, ok := reader.ReadBytes('\n')
	if ok != nil{
		fmt.Fprintf(os.Stderr, "Error in reading from socket in Write\n")
		fmt.Printf("Error in reading from socket in Write\n")
		debug.PrintStack()
		os.Exit(2)
	}
	arr := strings.Fields(string(resp))

	//expect(t, arr[0], "CONTENTS")
	if arr[0] != "CONTENTS"{
		fmt.Fprintf(os.Stderr, "Expected: %v found: %v in Read 1\n","CONTENTS",arr[0])
		fmt.Printf("Expected: %v found: %v in Read 1\n","CONTENTS",arr[0])
		debug.PrintStack()
		os.Exit(2)
	}
	//expect(t, arr[1], fmt.Sprintf("%v", version)) 
	if arr[1] != fmt.Sprintf("%v", version){
		fmt.Fprintf(os.Stderr, "Expected: %v found: %v in Read 2\n",fmt.Sprintf("%v", version),arr[1])
		fmt.Printf("Expected: %v found: %v in Read 2\n",fmt.Sprintf("%v", version),arr[1])
		debug.PrintStack()
		os.Exit(2)
	}
	//expect(t, arr[2], fmt.Sprintf("%v", len(contents)))	
	if arr[2] != fmt.Sprintf("%v", len(contents)){
		fmt.Fprintf(os.Stderr,"Expected: %v found: %v in Read 3\n",fmt.Sprintf("%v", len(contents)),arr[2])
		fmt.Printf("Expected: %v found: %v in Read 3\n",fmt.Sprintf("%v", len(contents)),arr[2])
		debug.PrintStack()
		os.Exit(2)
	}

    var contentRcvd = make([]byte,len(contents)+2)
    n, ok := reader.Read(contentRcvd)
	if ok != nil{
		fmt.Fprintf(os.Stderr, "Error in reading from socket in ReadSuccess\n")
		fmt.Printf("Error in reading from socket in ReadSuccess\n")
		debug.PrintStack()
		os.Exit(2)
	}
    if n != len(contents)+2{
		fmt.Fprintf(os.Stderr, "n and actual contents length not similar")
		fmt.Printf("n and actual contents length not similar")
		debug.PrintStack()
		os.Exit(2)
    }
    contentRcvdStr := string(contentRcvd)
    clen := len(contentRcvdStr)
    contentRcvdStr = contentRcvdStr[0:clen-2]
	//expect(t, contents, contentRcvdStr)
	if contents != contentRcvdStr{
		fmt.Fprintf(os.Stderr, "Expected: %v found: %v in Read 4\n",fmt.Sprintf("%v", len(contents)),arr[2])
		fmt.Printf("Expected: %v found: %v in Read 4\n",fmt.Sprintf("%v", len(contents)),arr[2])
		debug.PrintStack()
		os.Exit(2)
	}
}

func  ReadFailure(conn net.Conn, filename string, errstr string) {
    reader := bufio.NewReader(conn)
	fmt.Fprintf(conn, "read %v\r\n", filename)
	resp, ok := reader.ReadBytes('\n')
	if ok != nil{
		fmt.Fprintf(os.Stderr, "Error in reading from socket in ReadFailure\n")
		fmt.Printf("Error in reading from socket in ReadFailure\n")
		debug.PrintStack()
		os.Exit(2)
	}
	arr := strings.Fields(string(resp))
	//expect(t, arr[0], errstr)
	if arr[0] != errstr{
		fmt.Fprintf(os.Stderr, "Expected: %v found: %v in ReadFailure\n",errstr,arr[0])
		fmt.Printf("Expected: %v found: %v in ReadFailure\n",errstr,arr[0])
		debug.PrintStack()
		os.Exit(2)
	}
}

func  CasSuccess(conn net.Conn, filename string, contents string, version int64, exptime int) int64{
    reader := bufio.NewReader(conn)
	fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n", filename, version,len(contents), exptime, contents)
	resp, ok := reader.ReadBytes('\n')
	if ok != nil{
		fmt.Fprintf(os.Stderr, "Error in reading from socket in CasSuccess\n")
		fmt.Printf("Error in reading from socket in CasSuccess\n")
		debug.PrintStack()
		os.Exit(2)
	}
	arr := strings.Fields(string(resp)) 
	//expect(t, arr[0], "OK")
	if arr[0] != "OK"{
		fmt.Fprintf(os.Stderr, "Expected: %v found: %v in CasSuccess\n","OK",arr[0])
		fmt.Printf("Expected: %v found: %v in CasSuccess\n","OK",arr[0])
		debug.PrintStack()
		os.Exit(2)
	}
	version, err := strconv.ParseInt(arr[1], 10, 64) 
	if err != nil {
		fmt.Fprintf(os.Stderr, "Non-numeric version found")
		fmt.Printf("Non-numeric version found")
		debug.PrintStack()
		os.Exit(2)
	}
	return version 
}

func  CasFailure(conn net.Conn, filename string, contents string, incorrect_version int64,correct_version int64, exptime int, errstr string){
    reader := bufio.NewReader(conn)
	fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n", filename, incorrect_version,len(contents), exptime, contents)
	resp, ok := reader.ReadBytes('\n')
	if ok != nil{
		fmt.Fprintf(os.Stderr, "Error in reading from socket in CasFailure\n")
		fmt.Printf("Error in reading from socket in CasFailure\n")
		debug.PrintStack()
		os.Exit(2)
	}
	arr := strings.Fields(string(resp)) 
	//expect(t, arr[0], errstr)
	if arr[0] != errstr{
		fmt.Printf("Expected: %v found: %v in CasFailure\n",errstr,arr[0])
		debug.PrintStack()
		os.Exit(2)
	}
	//expect(t, arr[1], fmt.Sprintf("%v",version))
	if arr[1] != fmt.Sprintf("%v", correct_version){
		fmt.Fprintf(os.Stderr, "Expected: %v found: %v in CasFailure\n",fmt.Sprintf("%v", correct_version),arr[1])
		fmt.Printf("Expected: %v found: %v in CasFailure\n",fmt.Sprintf("%v", correct_version),arr[1])
		debug.PrintStack()
		os.Exit(2)
	}
}

func  DeleteSuccess(conn net.Conn, filename string){
    reader := bufio.NewReader(conn)
	fmt.Fprintf(conn, "delete %v\r\n", filename)
	resp, ok := reader.ReadBytes('\n')
	if ok != nil{
		fmt.Fprintf(os.Stderr, "Error in reading from socket in DeleteSuccess\n")
		fmt.Printf("Error in reading from socket in DeleteSuccess\n")
		os.Exit(2)
	}
	arr := strings.Fields(string(resp))
	//expect(t, arr[0], "OK")
	if arr[0] != "OK"{
		fmt.Fprintf(os.Stderr, "Expected: %v found: %v in DeleteSuccess\n","OK",arr[0])
		fmt.Printf("Expected: %v found: %v in DeleteSuccess\n","OK",arr[0])
		os.Exit(2)
	}
}

func TestBasic(t *testing.T){
	if !TESTBASIC{
		return
	}
	pubaddrs := make([]string, 5)
	pubaddrs[0] = "localhost:8006"
	pubaddrs[1] = "localhost:8007"
	pubaddrs[2] = "localhost:8008"
	pubaddrs[3] = "localhost:8009"
	pubaddrs[4] = "localhost:8010"
    fsnodes,_ := makeFsNodes(5, "input_spec.json", "log", 350, 450, pubaddrs)	
	//To get one node  elected as Leader
	time.Sleep(2*time.Second)
	fsnodes[0].smLock.RLock()
	ldrId := fsnodes[0].LeaderId()
	fsnodes[0].smLock.RUnlock()

	conn := GetConnection(t, fsnodes[ldrId-1].pubaddr)

	// Write Test
	name := "hello.txt"
	contents := "hello world"
	exptime := 3000000
	version := Write(conn, name, contents, exptime)

	//Read Test
	ReadSuccess(conn, name, contents, version)

	for _, node := range fsnodes{
		node.Shutdown()
	}
	time.Sleep(2*time.Second)
}

func TestSequential(t *testing.T){
	if !TESTSEQUENTIAL{
		return
	}
	pubaddrs := make([]string, 5)
	pubaddrs[0] = "localhost:8006"
	pubaddrs[1] = "localhost:8007"
	pubaddrs[2] = "localhost:8008"
	pubaddrs[3] = "localhost:8009"
	pubaddrs[4] = "localhost:8010"
    fsnodes,_ := makeFsNodes(5, "input_spec.json", "log", 350, 450, pubaddrs)	
	//To get one node  elected as Leader
	time.Sleep(2*time.Second)
	fsnodes[0].smLock.RLock()
	ldrId := fsnodes[0].LeaderId()
	fsnodes[0].smLock.RUnlock()

	conn := GetConnection(t, fsnodes[ldrId-1].pubaddr)

	// Write Test
	name := "hello.txt"
	contents := "hello world"
	exptime := 3000000
	version := Write(conn, name, contents, exptime)

	//Read Test
	ReadSuccess(conn, name, contents, version)

	//CAS Case 1 Test
	new_contents := "hello mortals"
	version = CasSuccess(conn, name, new_contents, version, exptime)

	//CAS Case 2 Test
	CasFailure(conn, name, new_contents, version-1,version, exptime, "ERR_VERSION") 

	//Delete Test
	DeleteSuccess(conn, name)

	//Read Fail Test
	ReadFailure(conn, name, "ERR_FILE_NOT_FOUND")
	name2 := "file2.txt"
	contents2 := "abcdef"
	version2 := Write(conn, name2, contents2, exptime)
	//Read Test
	ReadSuccess(conn, name2, contents2, version2)

	for _, node := range fsnodes{
		node.Shutdown()
	}
}


func TestConcurrent(t *testing.T){
	if !TESTCONCURRENT{
		return
	}
	pubaddrs := make([]string, 5)
	pubaddrs[0] = "localhost:8006"
	pubaddrs[1] = "localhost:8007"
	pubaddrs[2] = "localhost:8008"
	pubaddrs[3] = "localhost:8009"
	pubaddrs[4] = "localhost:8010"
    fsnodes,_ := makeFsNodes(5, "input_spec.json", "log", 350, 450, pubaddrs)	
	//To get one node  elected as Leader
	time.Sleep(2*time.Second)
	fsnodes[0].smLock.RLock()
	ldrId := fsnodes[0].LeaderId()
	fsnodes[0].smLock.RUnlock()

	conns := []net.Conn{}
	for i:=1;i<=2;i++{
		conn := GetConnection(t, fsnodes[ldrId-1].pubaddr)
		conns = append(conns, conn)
	}

	exptime := 3000000
	filename := "concurr_file"
	var wg sync.WaitGroup
	for idx, conn := range conns{

		wg.Add(1)
		go func(idx int, conn net.Conn){
			contents := fmt.Sprintf("Duckduckgo_%v",idx+1) 
			_ = Write(conn, filename, contents, exptime)
			wg.Done()
		}(idx, conn)
	}
	wg.Wait()

	time.Sleep(3*time.Second)			
	//Leader Failure
	running := make(map[int]bool)
	for i:=1;i<=5;i++{
		running[i] = true
	}
	fsnodes[0].smLock.RLock()
	lid := fsnodes[0].LeaderId()
	fsnodes[0].smLock.RUnlock()
	debugFsnTest(fmt.Sprintf("leader Id:%v\n", lid))
	if lid != -1{
		fsnodes[lid-1].Shutdown()
		debugFsnTest(fmt.Sprintf("Leader(id:%v) is down, now\n", lid))
		time.Sleep(10*time.Second)			
		
		for _, node := range fsnodes{
			node.smLock.RLock()
			debugFsnTest(fmt.Sprintf("Id(%v) LeaderId(%v) LastCommit(%v) IsOne(%v)\n", node.Id(), node.LeaderId(), node.sm.commitIndex, node.isOn))
			node.smLock.RUnlock()
		}
		running[lid]  = false
		var newldrId int = -1
		for i := 1; i<= 5;i++{
			if running[i]{
				fsnodes[i-1].smLock.RLock()
				newldrId = fsnodes[i-1].LeaderId()
				fsnodes[i-1].smLock.RUnlock()
				debugFsnTest(fmt.Sprintf("New leader Id:%v\n", newldrId))
				if(newldrId != lid){
					break
				}
			}
		}
		new_contents := "Final Contents"
		newconn := GetConnection(t, fsnodes[newldrId-1].pubaddr)
		version := Write(newconn, filename, new_contents, exptime)
		ReadSuccess(newconn, filename, new_contents, version)
	}

	for i, node := range fsnodes{
		if running[i+1]{
			node.Shutdown()
		}
	}
}
