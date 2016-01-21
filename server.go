package main
import (
	"net"
	"os"
	"fmt"
	"bufio"
	"strings"
	"strconv"
	"io/ioutil"
	"sync"
)

var mapMutex = struct{
	sync.RWMutex
	vmap map[string]int64
	emap map[string]int64
}{vmap: make(map[string]int64), emap: make(map[string]int64)}

func printErr(err error){
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
var flag int = 1
func debug(s string){
	if flag == 1 {
		fmt.Println(s)
	}
}

func handleSocket(conn net.Conn){
	defer conn.Close()
	response := ""
	scanner := bufio.NewScanner(conn)
	if err != nil{
		response = "ERR_INTERNAL \r\n"
		conn.Write([]byte(response))
		return
	}

	str := string(buf)
	commands := strings.Fields(str)
	command := commands[0]
	if len(commands) < 2 {
		response = "ERR_CMD_ERR \r\n"
		conn.Write([]byte(response))
		return
	}
	filename := commands[1]
	debug(fmt.Sprintf("command:%v", command))
	if command == "cas" {
		if len(commands) < 4 {
			debug("Compare and swap: insufficient arguments\n")
			response =  "ERR_CMD_ERR\r\n"
			conn.Write([]byte(response))
			return
		}
		vers,err := strconv.ParseInt(commands[2], 10, 64)
		if err != nil {
			response = "ERR_CMD_ERR \r\n"
			conn.Write([]byte(response))
			return
		}

		numbytes,err := strconv.ParseInt(commands[3], 10, 64)
		if err != nil {
			response = "ERR_CMD_ERR \r\n"
			conn.Write([]byte(response))
			return
		}

		var exptime int64 = -1
		if len(commands) == 5 {
			exptime,err = strconv.ParseInt(commands[4], 10, 64)
			if err != nil {
				response = "ERR_CMD_ERR \r\n"
				conn.Write([]byte(response))
				return
			}
		}
		//response, ok = handleCas(filename, ver, numbytes, exptime)
		content, ok := reader.Read()
		if int64(len(content)) != numbytes{
			debug(fmt.Sprintf("Number of bytes %v not equal to length of content %v in CAS\n", numbytes, len(content)))
		}
		mapMutex.RLock()
		curr := mapMutex.vmap[filename]
		mapMutex.RUnlock()
		if curr != vers {
			mapMutex.RUnlock()
			response = fmt.Sprintf("ERR_VERSION %v\r\n", curr)
			conn.Write([]byte(response))
			return
		}
		const modePerm os.FileMode = 0755
		ok = ioutil.WriteFile(filename,  content, modePerm)
		if ok != nil {
			mapMutex.RUnlock()
			response = "ERR_INTERNAL \r\n"
			conn.Write([]byte(response))
			return
		}
		mapMutex.vmap[filename] = curr +1	
		mapMutex.emap[filename] = exptime
		mapMutex.RUnlock()
		response = fmt.Sprintf("OK %v\r\n", curr+1)

	}else if command == "read" {
		if len(commands) != 2 {
			debug(fmt.Sprintf("Read: number of arguments:%d", len(commands)))
			response =  "ERR_CMD_ERR\r\n"
			conn.Write([]byte(response))
			return
		}
		mapMutex.RLock()
		vers, present :=  mapMutex.vmap[filename]
		if !present {
			response = "ERR_FILE_NOT_FOUND\r\n"
			conn.Write([]byte(response))
			return
		}
		exptime, _ := mapMutex.emap[filename]
		mapMutex.RUnlock()

		content, ok := ioutil.ReadFile(filename)
		if ok != nil {
			response = "ERR_FILE_NOT_FOUND\r\n"
			conn.Write([]byte(response))
			return
		}
		temp := fmt.Sprintf("CONTENTS %v %v %v \r\n",vers, len(content), exptime)
		conn.Write([]byte(temp))
		conn.Write(content)
	}else if command == "write" {
		if len(commands) < 3 {
			debug(fmt.Sprintf("Read: number of arguments:%d", len(commands)))
			response =  "ERR_CMD_ERR\r\n"
			conn.Write([]byte(response))
			return
		}
		numbytes,err := strconv.ParseInt(commands[2], 10, 64)
		if err != nil {
			response = "ERR_CMD_ERR \r\n"
			conn.Write([]byte(response))
			return
		}
		var exptime int64 = -1
		if len(commands) == 4 {
			exptime,err = strconv.ParseInt(commands[3], 10, 64)
			if err != nil {
				response = "ERR_CMD_ERR \r\n"
				conn.Write([]byte(response))
				return
			}
		}
		//response, ok = handleWrite(filename, numbytes, exptime)
		debug(fmt.Sprintf("In write"))
		content, ok := reader.ReadBytes('\n')
		fmt.Println("ok:",ok)
		debug(fmt.Sprintf("filename:%v\n", filename))
		debug(fmt.Sprintf("numbytes:%v\n", numbytes))
		debug(fmt.Sprintf("exptime:%v\n", exptime))
		debug(fmt.Sprintf("Content:%v\n", string(content)))
		if int64(len(content)) != numbytes{
			debug(fmt.Sprintf("Number of bytes %v not equal to length of content(%v) %v in write\n", numbytes,string(content), len(content)))
		}
		mapMutex.Lock()
		vers , present := mapMutex.vmap[filename]
		if present {
			vers += 1
		}else {
			vers = 0
		}
		const modePerm os.FileMode = 0755
		ok = ioutil.WriteFile(filename,  content, modePerm)
		if ok != nil {
			mapMutex.Unlock()
			response = "ERR_INTERNAL \r\n"
			conn.Write([]byte(response))
			return
		}
		mapMutex.emap[filename] = exptime
		mapMutex.vmap[filename] = vers
		mapMutex.Unlock()
		response = fmt.Sprintf("OK %v \r\n", vers)
	}else if command == "delete" {
		if len(commands) != 2 {
			fmt.Printf("Read: number of arguments:%d", len(commands))
			response =  "ERR_CMD_ERR\r\n"
			conn.Write([]byte(response))
			return
		}
		err := os.Remove(filename)
		if err != nil {
			response = "ERR_INTERNAL \r\n"
			conn.Write([]byte(response))
			return
		}
		mapMutex.Lock()
		delete(mapMutex.vmap, filename)
		delete(mapMutex.emap, filename)
		mapMutex.Unlock()
		response = "OK\r\n"
		//response,ok = handleDelete(filename)
	}else{
		debug("Unknown command\n")
		response =  "ERR_CMD_ERR\r\n"
		conn.Write([]byte(response))
		return
	}
	conn.Write([]byte(response))
	return
}

func handleCas(filename string,version float64, numbytes int64, exptime int64){
	fmt.Println("In handleCas")
}

func handleRead(filename string){
	fmt.Println("In handleRead")
}

func handleWrite(filename string, numbytes int64, exptime int64){
	fmt.Println("In handleWrite")
}

func handleDelete(fliename string){
	fmt.Println("In handleDelete")
}

func serverMain() {
	serverid := "127.0.0.1:8080"
	if len(os.Args) == 2 {
		serverid = os.Args[1]
	}
	addr, err := net.ResolveTCPAddr("tcp4", serverid)
	printErr(err)

	socket, err :=  net.ListenTCP("tcp", addr)
	printErr(err)

	for {
		conn, err := socket.Accept()
		if err != nil {
			continue
		}
		go handleSocket(conn)
	}
}

func main(){
	serverMain()
}
