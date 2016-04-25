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
    "errors"
)
var DebugServer bool = true //Turn on/off debugServer statements 

func debugServer(s string){
	if DebugServer {
		fmt.Println(s)
	}
}

type FileMap struct{
    sync.RWMutex
    dict map[string]*fileStruct
}

type fileStruct struct{
    version int64 
    exptime time.Time
    contents []byte
}

func (fs *fileStruct) String() string{
    s := fmt.Sprintf("version(%v) exptime(%v) contents(%v)", fs.version, fs.exptime, fs.contents)
    return s
}

var fileMap = &FileMap{dict:make(map[string]*fileStruct, 1000)}

func printErr(err error){
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

var cmderr = errors.New("ERR_CMD_ERR")
var verserr = errors.New("ERR_VERSION") 
var fnferr = errors.New("ERR_FILE_NOT_FOUND") 
var interr = errors.New("ERR_INTERNAL") 

func handleCas(firstline string, conn net.Conn, reader *bufio.Reader) error{
    commands := strings.Fields(firstline)
    filename := commands[1]
    if len(commands) < 4 {
        debugServer("Compare and swap: insufficient arguments\n")
        fmt.Fprintf(conn, "%v \r\n", cmderr)
        conn.Close()
        return cmderr
    }
    vers,err := strconv.ParseInt(commands[2], 10, 64)
    if err != nil {
        fmt.Fprintf(conn, "%v \r\n", cmderr)
        conn.Close()
        return cmderr
    }

    numbytes,err := strconv.ParseInt(commands[3], 10, 64)
    if err != nil {
        fmt.Fprintf(conn, "%v \r\n", cmderr)
        conn.Close()
        return cmderr
    }

    var exptime int64 = -1
    if len(commands) == 5 {
        exptime,err = strconv.ParseInt(commands[4], 10, 64)
        if err != nil {
            fmt.Fprintf(conn, "%v \r\n", cmderr)
            conn.Close()
            return cmderr 
        }
    }

    var content = make([]byte,numbytes+2)
    n, ok := reader.Read(content)
    if len(content) != n{
        debugServer(fmt.Sprintf("Number of bytes %v not equal to length of content %v in CAS\n", n, len(content)))
    }
    fileMap.RLock()
    curr := fileMap.dict[filename].version
    if curr != vers {
        fileMap.RUnlock()
        fmt.Fprintf(conn, "%v %v \r\n",verserr, curr)
        return verserr
    }
    const modePerm os.FileMode = 0755
    ok = ioutil.WriteFile(filename,  content, modePerm)
    if ok != nil {
        fileMap.RUnlock()
        fmt.Fprintf(conn, "%v \r\n", interr)
        return interr
    }
    fileMap.dict[filename].version = curr +1	
    fileMap.dict[filename].exptime = time.Now().Add(time.Duration(exptime)*time.Millisecond)
    fileMap.RUnlock()
    fmt.Fprintf(conn,"OK %v \r\n", curr+1)
    return nil
}

func handleRead(firstline string, conn net.Conn) error{
    fmt.Println(firstline)
    tokens := strings.Fields(firstline)
    filename := tokens[1]
    if len(tokens) != 2 {
        debugServer(fmt.Sprintf("Read: number of arguments:%d", len(tokens)))
        fmt.Fprintf(conn,"%v \r\n", cmderr)
        conn.Close()
        return cmderr
    }
    fileMap.RLock()
    _, present := fileMap.dict[filename] 
    if !present {
        fmt.Fprintf(conn,"%v \r\n", fnferr)
		fileMap.RUnlock()
        return fnferr
    }
    vers :=  fileMap.dict[filename].version
    expirytime := fileMap.dict[filename].exptime
    fileMap.RUnlock()

    expired := !(expirytime.Sub(time.Now()) > time.Nanosecond)
    fmt.Printf("filename:%v version:%v exptime:%v time.Now():%v expired:%v\n", filename, vers, expirytime,time.Now(), expired)
    if expired {
        fmt.Fprintf(conn,"%v \r\n", fnferr)
        _ = os.Remove(filename)
        fileMap.Lock()
        delete(fileMap.dict, filename)
        fileMap.Unlock()
        return fnferr
    }

    content, ok := ioutil.ReadFile(filename)
    if ok != nil {
        fmt.Fprintf(conn,"%v \r\n", fnferr)
        return fnferr
    }
    fmt.Fprintf(conn,"CONTENTS %v %v %v  \r\n",vers, len(content)-2, expirytime)
    conn.Write(content)
    return nil
}

func handleWrite(firstline string, conn net.Conn, reader *bufio.Reader) error{
    tokens := strings.Fields(firstline)
    filename := tokens[1]
    if len(tokens) < 3 {
        debugServer(fmt.Sprintf("Read: number of arguments:%d", len(tokens)))
        fmt.Fprintf(conn,"%v \r\n", cmderr)
        conn.Close()
        return cmderr
    }
    numbytes,err := strconv.ParseInt(tokens[2], 10, 64)
    if err != nil {
        fmt.Fprintf(conn,"%v \r\n", cmderr)
        conn.Close()
        return cmderr
    }
    var expirytime int64 = -1
    if len(tokens) == 4 {
        expirytime,err = strconv.ParseInt(tokens[3], 10, 64)
        if err != nil {
            fmt.Fprintf(conn,"%v \r\n", cmderr)
            conn.Close()
            return cmderr
        }
    }
    var content = make([]byte,numbytes+2)
    n, ok := reader.Read(content)
    if len(content) != n{
        debugServer(fmt.Sprintf("Number of bytes %v not equal to length of content %v in Write\n", n, len(content)))
    }
	debugServer(fmt.Sprintf("In write: contents received:%v\n", string(content)))
    fileMap.Lock()
	debugServer(fmt.Sprintf("fileMap locked\n"))
    var vers int64 = 0
    if _, present := fileMap.dict[filename]; present {
        vers = fileMap.dict[filename].version + 1
    }
	debugServer(fmt.Sprintf("Version:%v\n", vers))
    const modePerm os.FileMode = 0755
    ok = ioutil.WriteFile(filename,  content, modePerm)
    if ok != nil {
        fileMap.Unlock()
        fmt.Fprintf(conn,"%v \r\n", interr)
        return interr
    }
    fileMap.dict[filename] = &fileStruct{}
    fileMap.dict[filename].exptime = time.Now().Add(time.Duration(expirytime)*time.Millisecond)
    fileMap.dict[filename].version = vers
    fmt.Printf("Write filename(%v) version(%v) exptime(%v)\n", filename, fileMap.dict[filename].version, fileMap.dict[filename].exptime)
    fileMap.Unlock()
    fmt.Fprintf(conn,"OK %v  \r\n", vers)
    return nil
}

func handleDelete(firstline string, conn net.Conn) error{
	tokens := strings.Fields(firstline)
	filename := tokens[1]
	if len(tokens) != 2 {
		debugServer(fmt.Sprintf("Read: number of arguments:%d", len(tokens)))
		fmt.Fprintf(conn,"%v \r\n", cmderr)
		conn.Close()
		return cmderr
	}

	err := os.Remove(filename)
	if err != nil {
		fmt.Printf("%v\n", err)
		fmt.Fprintf(conn,"%v \r\n", interr)
		return interr
	}
	fileMap.Lock()
	delete(fileMap.dict, filename)
	fileMap.Unlock()
	fmt.Fprintf(conn,"OK \r\n")
	return nil
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

		firstline := string(buf)
		commands := strings.Fields(firstline)
		//fmt.Println(commands, len(commands))
		command := commands[0]
		if len(commands) < 2 {
			fmt.Fprintf(conn, "ERR_CMD_ERR \r\n")
			conn.Close()
			return
		}
		debugServer(fmt.Sprintf("command:%v", command))
		
		switch command {
			case "cas": {
                err = handleCas(firstline, conn, reader)
            }
			case "read": {
                err = handleRead(firstline, conn)
			}
			case "write": {
				err = handleWrite(firstline, conn, reader)
			}
			case "delete": {
				err = handleDelete(firstline, conn)
			}
			default: {
				debugServer("Unknown command\n")
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
