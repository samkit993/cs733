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
	exptime := 300000
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		t.Error(err.Error()) 
	}

	scanner := bufio.NewScanner(conn)

	// Write Test
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime, contents)
	scanner.Scan() 
	resp := scanner.Text() 
	arr := strings.Split(resp, " ") 
	expect(t, arr[0], "OK")
	version, err := strconv.ParseInt(arr[1], 10, 64) 
	if err != nil {
		t.Error("Non-numeric version found")
	}

	//Read Test
	fmt.Fprintf(conn, "read %v\r\n", name) 
	scanner.Scan()
	arr = strings.Split(scanner.Text(), " ")
	expect(t, arr[0], "CONTENTS")
	expect(t, arr[1], fmt.Sprintf("%v", version)) 
	expect(t, arr[2], fmt.Sprintf("%v", len(contents)))	
	scanner.Scan()
	expect(t, contents, scanner.Text())

	//CAS Case 1 Test
	new_contents := "hello stupid world"
	fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n", name, version,len(new_contents), exptime, new_contents)
	scanner.Scan()
	resp = scanner.Text() 
	arr = strings.Split(resp, " ") 
	expect(t, arr[0], "OK")
	version, err = strconv.ParseInt(arr[1], 10, 64) 
	if err != nil {
		t.Error("Non-numeric version found")
	}

	//CAS Case 2 Test
	fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n", name, version-1,len(new_contents), exptime, new_contents)
	scanner.Scan()
	resp = scanner.Text() 
	arr = strings.Split(resp, " ") 
	expect(t, arr[0], "ERR_VERSION")
	expect(t, arr[1], fmt.Sprintf("%v",version))

	//Delete Test
	fmt.Fprintf(conn, "delete %v\r\n", name)
	scanner.Scan()
	resp = scanner.Text()
	arr = strings.Split(resp, " ")
	expect(t, arr[0], "OK")

	//Read Fail Test
	fmt.Fprintf(conn, "read %v\r\n", name)
	scanner.Scan()
	resp = scanner.Text()
	arr = strings.Split(resp, " ")
	expect(t, arr[0], "ERR_FILE_NOT_FOUND")
}

