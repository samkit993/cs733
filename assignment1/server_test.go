package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"sync"
	"os"
	"runtime/debug"
)

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
	if ok != nil{
		fmt.Fprintf(os.Stderr, "Error in reading from socket in Write\n")
		debug.PrintStack()
		os.Exit(1)
	}
	arr := strings.Fields(string(resp)) 
	if arr[0] != "OK"{
		fmt.Fprintf(os.Stderr, "Expected: %v found: %v in Write 1\n","OK", arr[0])
		debug.PrintStack()
		os.Exit(1)
	}
	version, err := strconv.ParseInt(arr[1], 10, 64) 
	if err != nil {
		fmt.Fprintf(os.Stderr, "Non-numeric version found in Write")
		debug.PrintStack()
		os.Exit(1)
	}
	return version 
}

func  ReadSuccess(conn net.Conn, filename string, contents string, version int64){
    reader := bufio.NewReader(conn)
	fmt.Fprintf(conn, "read %v\r\n", filename)
	resp, ok := reader.ReadBytes('\n')
	if ok != nil{
		fmt.Fprintf(os.Stderr, "Error in reading from socket in Write\n")
		debug.PrintStack()
		os.Exit(1)
	}
	arr := strings.Fields(string(resp))

	//expect(t, arr[0], "CONTENTS")
	if arr[0] != "CONTENTS"{
		fmt.Fprintf(os.Stderr, "Expected: %v found: %v in Read 1\n","CONTENTS",arr[0])
		debug.PrintStack()
		os.Exit(1)
	}
	//expect(t, arr[1], fmt.Sprintf("%v", version)) 
	if arr[1] != fmt.Sprintf("%v", version){
		fmt.Fprintf(os.Stderr, "Expected: %v found: %v in Read 2\n",fmt.Sprintf("%v", version),arr[1])
		debug.PrintStack()
		os.Exit(1)
	}
	//expect(t, arr[2], fmt.Sprintf("%v", len(contents)))	
	if arr[2] != fmt.Sprintf("%v", len(contents)){
		fmt.Fprintf(os.Stderr,"Expected: %v found: %v in Read 3\n",fmt.Sprintf("%v", len(contents)),arr[2])
		debug.PrintStack()
		os.Exit(1)
	}

    var contentRcvd = make([]byte,len(contents)+2)
    n, ok := reader.Read(contentRcvd)
	if ok != nil{
		fmt.Fprintf(os.Stderr, "Error in reading from socket in ReadSuccess\n")
		debug.PrintStack()
		os.Exit(1)
	}
    if n != len(contents)+2{
		fmt.Fprintf(os.Stderr, "n and actual contents length not similar")
		debug.PrintStack()
		os.Exit(1)
    }
    contentRcvdStr := string(contentRcvd)
    clen := len(contentRcvdStr)
    contentRcvdStr = contentRcvdStr[0:clen-2]
	//expect(t, contents, contentRcvdStr)
	if contents != contentRcvdStr{
		fmt.Fprintf(os.Stderr, "Expected: %v found: %v in Read 4\n",fmt.Sprintf("%v", len(contents)),arr[2])
		debug.PrintStack()
		os.Exit(1)
	}
}

func  ReadFailure(conn net.Conn, filename string, errstr string) {
    reader := bufio.NewReader(conn)
	fmt.Fprintf(conn, "read %v\r\n", filename)
	resp, ok := reader.ReadBytes('\n')
	if ok != nil{
		fmt.Fprintf(os.Stderr, "Error in reading from socket in ReadFailure\n")
		debug.PrintStack()
		os.Exit(1)
	}
	arr := strings.Fields(string(resp))
	//expect(t, arr[0], errstr)
	if arr[0] != errstr{
		fmt.Fprintf(os.Stderr, "Expected: %v found: %v in ReadFailure\n",errstr,arr[0])
		debug.PrintStack()
		os.Exit(1)
	}
}

func  CasSuccess(conn net.Conn, filename string, contents string, version int64, exptime int) int64{
    reader := bufio.NewReader(conn)
	fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n", filename, version,len(contents), exptime, contents)
	resp, ok := reader.ReadBytes('\n')
	if ok != nil{
		fmt.Fprintf(os.Stderr, "Error in reading from socket in CasSuccess\n")
		debug.PrintStack()
		os.Exit(1)
	}
	arr := strings.Fields(string(resp)) 
	//expect(t, arr[0], "OK")
	if arr[0] != "OK"{
		fmt.Fprintf(os.Stderr, "Expected: %v found: %v in CasSuccess\n","OK",arr[0])
		debug.PrintStack()
		os.Exit(1)
	}
	version, err := strconv.ParseInt(arr[1], 10, 64) 
	if err != nil {
		fmt.Fprintf(os.Stderr, "Non-numeric version found")
		debug.PrintStack()
		os.Exit(1)
	}
	return version 
}

func  CasFailure(conn net.Conn, filename string, contents string, incorrect_version int64,correct_version int64, exptime int, errstr string){
    reader := bufio.NewReader(conn)
	fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n", filename, incorrect_version,len(contents), exptime, contents)
	resp, ok := reader.ReadBytes('\n')
	if ok != nil{
		fmt.Fprintf(os.Stderr, "Error in reading from socket in CasFailure\n")
		debug.PrintStack()
		os.Exit(1)
	}
	arr := strings.Fields(string(resp)) 
	//expect(t, arr[0], errstr)
	if arr[0] != errstr{
		fmt.Fprintf(os.Stderr,"Expected: %v found: %v in CasFailure\n",errstr,arr[0])
		debug.PrintStack()
		os.Exit(1)
	}
	//expect(t, arr[1], fmt.Sprintf("%v",version))
	if arr[1] != fmt.Sprintf("%v", correct_version){
		fmt.Fprintf(os.Stderr, "Expected: %v found: %v in CasFailure\n",fmt.Sprintf("%v", correct_version),arr[1])
		debug.PrintStack()
		os.Exit(1)
	}
}

func  DeleteSuccess(conn net.Conn, filename string){
    reader := bufio.NewReader(conn)
	fmt.Fprintf(conn, "delete %v\r\n", filename)
	resp, ok := reader.ReadBytes('\n')
	if ok != nil{
		fmt.Fprintf(os.Stderr, "Error in reading from socket in DeleteSuccess\n")
		os.Exit(1)
	}
	arr := strings.Fields(string(resp))
	//expect(t, arr[0], "OK")
	if arr[0] != "OK"{
		fmt.Fprintf(os.Stderr, "Expected: %v found: %v in DeleteSuccess\n","OK",arr[0])
		os.Exit(1)
	}
}

func TestSequential(t *testing.T) {
	//fmt.Println(name, contents)
	conn := GetConnection(t, "localhost:8080")
	defer conn.Close()

	// Write Test
	name := "hello.txt"
	contents := "hello \r\n world"
	exptime := 300000
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
}

func TestBinary(t *testing.T) {
	conn := GetConnection(t, "localhost:8080")
	defer conn.Close()

	// Write binary contents
	contents := "\x00\x01\r\n\x03" // some non-ascii, some crlf chars
	filename := "binfile"
	exptime := 3000000
	version := Write(conn, filename, contents, exptime)

	// Expect to read it back
	ReadSuccess(conn, filename, contents, version)
}


func TestConcurrent(t *testing.T){
	conns := []net.Conn{}
	for i:=1;i<=5;i++{
		conn := GetConnection(t, fmt.Sprintf("localhost:8080", i))
		conns = append(conns, conn)
	}

	var wg sync.WaitGroup
	filename := "concurr_file"
	for idx, conn := range conns{
		exptime := 3000000

		wg.Add(1)
		go func(idx int, conn net.Conn){
			contents := fmt.Sprintf("Duckduckgo_%v",idx+1) 
			_ = Write(conn, filename, contents, exptime)
			wg.Done()
		}(idx, conn)
	}
	wg.Wait()

	for _, conn := range conns{
		conn.Close()
	}
}
