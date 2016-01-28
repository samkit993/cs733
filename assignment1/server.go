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
	"time"
)

var mapMutex = struct{
	sync.RWMutex
	vmap map[string]int64
	emap map[string]int64
	tmap map[string]time.Time
}{vmap: make(map[string]int64), emap: make(map[string]int64), tmap: make(map[string]time.Time)}

var flag int = 0	//Turn on/off debug statements 

func debug(s string){
	if flag == 1 {
		fmt.Println(s)
	}
}

func checkExpiry(limit int64, stampedtime time.Time) bool {
	r := time.Duration(limit)*time.Millisecond
	exptime := time.Now().Add(r)
	if exptime.Sub(time.Now()) > time.Nanosecond {
		return false
	}else{
		return true
	}
}


func deleteFile(){

}

func printErr(err error){
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func handleSocket(conn net.Conn){
	reader := bufio.NewReader(conn)
	for{
		buf, err := reader.ReadBytes('\n')
		if err != nil{
			fmt.Fprintf(conn, "ERR_INTERNAL \r\n")
			conn.Close()
			return
		}

		str := string(buf)
		commands := strings.Fields(str)
		//fmt.Println(commands, len(commands))
		command := commands[0]
		if len(commands) < 2 {
			fmt.Fprintf(conn, "ERR_CMD_ERR \r\n")
			conn.Close()
			return
		}
		filename := commands[1]
		debug(fmt.Sprintf("command:%v", command))
		
		switch command {
			case "cas": {
				if len(commands) < 4 {
					debug("Compare and swap: insufficient arguments\n")
					fmt.Fprintf(conn, "ERR_CMD_ERR \r\n")
					conn.Close()
					return
				}
				vers,err := strconv.ParseInt(commands[2], 10, 64)
				if err != nil {
					fmt.Fprintf(conn, "ERR_CMD_ERR \r\n")
					conn.Close()
					return
				}

				numbytes,err := strconv.ParseInt(commands[3], 10, 64)
				if err != nil {
					fmt.Fprintf(conn, "ERR_CMD_ERR \r\n")
					conn.Close()
					return
				}

				var exptime int64 = -1
				if len(commands) == 5 {
					exptime,err = strconv.ParseInt(commands[4], 10, 64)
					if err != nil {
						fmt.Fprintf(conn, "ERR_CMD_ERR \r\n")
						conn.Close()
						return
					}
				}
				var content = make([]byte,numbytes+2)
				n, ok := reader.Read(content)
				if len(content) != n{
					debug(fmt.Sprintf("Number of bytes %v not equal to length of content %v in CAS\n", n, len(content)))
				}
				mapMutex.RLock()
				curr := mapMutex.vmap[filename]
				if curr != vers {
					mapMutex.RUnlock()
					fmt.Fprintf(conn, "ERR_VERSION %v \r\n",curr)
					continue
				}
				const modePerm os.FileMode = 0755
				ok = ioutil.WriteFile(filename,  content, modePerm)
				if ok != nil {
					mapMutex.RUnlock()
					fmt.Fprintf(conn, "ERR_INTERNAL \r\n")
					continue
				}
				mapMutex.vmap[filename] = curr +1	
				mapMutex.emap[filename] = exptime
				mapMutex.RUnlock()
				fmt.Fprintf(conn,"OK %v \r\n", curr+1)

			}
			case "read": {
				if len(commands) != 2 {
					debug(fmt.Sprintf("Read: number of arguments:%d", len(commands)))
					fmt.Fprintf(conn,"ERR_CMD_ERR \r\n")
					conn.Close()
					return
				}
				mapMutex.RLock()
				vers, present :=  mapMutex.vmap[filename]
				if !present {
					fmt.Fprintf(conn,"ERR_FILE_NOT_FOUND \r\n")
				}
				exptime, _ := mapMutex.emap[filename]
				stime, _ := mapMutex.tmap[filename]
				mapMutex.RUnlock()

				expired := checkExpiry(exptime, stime)
				if expired {
					fmt.Fprintf(conn,"ERR_FILE_NOT_FOUND \r\n")
					_ = os.Remove(filename)
					mapMutex.Lock()
					delete(mapMutex.vmap, filename)
					delete(mapMutex.emap, filename)
					delete(mapMutex.tmap, filename)
					mapMutex.Unlock()
					continue
				}

				content, ok := ioutil.ReadFile(filename)
				if ok != nil {
					fmt.Fprintf(conn,"ERR_FILE_NOT_FOUND \r\n")
					continue
				}
				fmt.Fprintf(conn,"CONTENTS %v %v %v  \r\n",vers, len(content)-2, exptime)
				conn.Write(content)

			}
			case "write": {
				if len(commands) < 3 {
					debug(fmt.Sprintf("Read: number of arguments:%d", len(commands)))
					fmt.Fprintf(conn,"ERR_CMD_ERR \r\n")
					conn.Close()
					return
				}
				numbytes,err := strconv.ParseInt(commands[2], 10, 64)
				if err != nil {
					fmt.Fprintf(conn,"ERR_CMD_ERR \r\n")
					conn.Close()
					return
				}
				var exptime int64 = -1
				if len(commands) == 4 {
					exptime,err = strconv.ParseInt(commands[3], 10, 64)
					if err != nil {
						fmt.Fprintf(conn,"ERR_CMD_ERR \r\n")
						conn.Close()
						return
					}
				}
				debug(fmt.Sprintf("In write"))
				var content = make([]byte,numbytes+2)
				n, ok := reader.Read(content)
				if len(content) != n{
					debug(fmt.Sprintf("Number of bytes %v not equal to length of content %v in Write\n", n, len(content)))
				}
				debug(fmt.Sprintf("filename:%v\n", filename))
				debug(fmt.Sprintf("numbytes:%v\n", numbytes))
				debug(fmt.Sprintf("exptime:%v\n", exptime))
				debug(fmt.Sprintf("Content:%v\n", string(content)))
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
					fmt.Fprintf(conn,"ERR_INTERNAL \r\n")
				continue
				}
				mapMutex.emap[filename] = exptime
				mapMutex.vmap[filename] = vers
				mapMutex.Unlock()
				fmt.Fprintf(conn,"OK %v  \r\n", vers)
			}

			case "delete": {
				if len(commands) != 2 {
					debug(fmt.Sprintf("Read: number of arguments:%d", len(commands)))
					fmt.Fprintf(conn,"ERR_CMD_ERR \r\n")
					conn.Close()
					return
				}
				err := os.Remove(filename)
				if err != nil {
					fmt.Fprintf(conn,"ERR_INTERNAL \r\n")
					continue
				}
				mapMutex.Lock()
				delete(mapMutex.vmap, filename)
				delete(mapMutex.emap, filename)
				delete(mapMutex.tmap, filename)
				mapMutex.Unlock()
				fmt.Fprintf(conn,"OK \r\n")

			}
			default: {
				debug("Unknown command\n")
				fmt.Fprintf(conn,"ERR_CMD_ERR \r\n")
				conn.Close()
				return
			}
		}
	}
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
