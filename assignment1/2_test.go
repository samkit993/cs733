package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
)

func TestTCPExtended(t *testing.T) {
	name := "hello.txt"
	contents := "hello \r\n world"
	//fmt.Println(name, contents)
	exptime := 300000
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		t.Error(err.Error()) 
	}

    reader := bufio.NewReader(conn)

	// Write Test
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime, contents)
    resp, ok := reader.ReadBytes('\n')
    checkError(t,ok)
	arr := strings.Fields(string(resp)) 
	expect(t, arr[0], "OK")
	version, err := strconv.ParseInt(arr[1], 10, 64) 
	if err != nil {
		t.Error("Non-numeric version found")
	}

	//Read Test
	fmt.Fprintf(conn, "read %v\r\n", name) 
    resp, ok = reader.ReadBytes('\n')
    checkError(t,ok)
	arr = strings.Fields(string(resp))
	expect(t, arr[0], "CONTENTS")
	expect(t, arr[1], fmt.Sprintf("%v", version)) 
	expect(t, arr[2], fmt.Sprintf("%v", len(contents)))	

    var contentRcvd = make([]byte,len(contents)+2)
    n, ok := reader.Read(contentRcvd)
    checkError(t,ok)
    if n != len(contents)+2{
        t.Error(t, fmt.Sprintf("%v",len(contents)+2), fmt.Sprintf("%v", n))
    }
    contentRcvdStr := string(contentRcvd)
    clen := len(contentRcvdStr)
    contentRcvdStr = contentRcvdStr[0:clen-2]
	expect(t, contents, contentRcvdStr)

	//CAS Case 1 Test
	new_contents := "hello stupid world"
	fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n", name, version,len(new_contents), exptime, new_contents)
    resp, ok = reader.ReadBytes('\n')
    checkError(t,ok)
	arr = strings.Fields(string(resp)) 
	expect(t, arr[0], "OK")
	version, err = strconv.ParseInt(arr[1], 10, 64) 
	if err != nil {
		t.Error("Non-numeric version found")
	}

	//CAS Case 2 Test
	fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n", name, version-1,len(new_contents), exptime, new_contents)
    resp, ok = reader.ReadBytes('\n')
    checkError(t,ok)
	arr = strings.Fields(string(resp)) 
	expect(t, arr[0], "ERR_VERSION")
	expect(t, arr[1], fmt.Sprintf("%v",version))

	//Delete Test
	fmt.Fprintf(conn, "delete %v\r\n", name)
    resp, ok = reader.ReadBytes('\n')
    checkError(t,ok)
	arr = strings.Fields(string(resp))
	expect(t, arr[0], "OK")

	//Read Fail Test
	fmt.Fprintf(conn, "read %v\r\n", name)
    resp, ok = reader.ReadBytes('\n')
    checkError(t,ok)
	arr = strings.Fields(string(resp))
	expect(t, arr[0], "ERR_FILE_NOT_FOUND")
}

