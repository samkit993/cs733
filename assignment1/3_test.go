package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"sync"
	"math/rand"
)

var mapMutexTest = struct{
	sync.RWMutex
	vmap map[string]int64
	emap map[string]int64
}{vmap: make(map[string]int64), emap: make(map[string]int64)}

func TestTCPConcurrent(t *testing.T) {
	name := "hello_"
	contents := "hello___world"

    for i := 0; i < 50; i++ {
		new_name := name + fmt.Sprintf("_%v.txt", i)
        new_contents := contents + fmt.Sprintf("_%v", i)
		//fmt.Println("In TCP concurrent",new_name, new_contents)
        go func(t *testing.T, name string, contents string){
			exptime := int64(rand.Intn(10))
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
			mapMutexTest.Lock()
			mapMutexTest.vmap[name] = version
			mapMutexTest.emap[name] = exptime
			mapMutexTest.Unlock()
		}(t, new_name, new_contents)
	}

    for i := 0; i < 50; i++ {
        new_name := name + fmt.Sprintf("%v.txt", i)
        new_contents := contents + fmt.Sprintf("_%v", i)
        go func(t *testing.T, name string, contents string){

			conn, err := net.Dial("tcp", "localhost:8080")
			if err != nil {
				t.Error(err.Error()) 
			}

			reader := bufio.NewReader(conn)
			//Read Test
			fmt.Fprintf(conn, "read %v\r\n", name) 
			resp, ok := reader.ReadBytes('\n')
			checkError(t,ok)
			arr := strings.Fields(string(resp))
			mapMutexTest.RLock()
			version := mapMutexTest.vmap[name]
			mapMutexTest.RUnlock()
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
			//fmt.Println(name, arr[1], contentRcvdStr)
			clen := len(contentRcvdStr)
			contentRcvdStr = contentRcvdStr[0:clen-2]
			expect(t, contents, contentRcvdStr)
		}(t, new_name, new_contents)
    }
}

