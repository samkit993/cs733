package main
import (
	"net"
	"os"
	"fmt"
	"bufio"
	"strings"
	"strconv"
    "errors"
	"time"
	"sync"
	"bytes"
	"encoding/gob"
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/cluster/mock"
	"github.com/cs733-iitb/log"
)
var DebugServer bool = false //Turn on/off debugServer statements 

func debugServer(s string){
	if DebugServer {
		fmt.Printf(s)
	}
}
/* Encoding/Decoding functions are taken from Sriram Sir's log package
 */
type EncodeHelper struct {
	Data interface{}
}

func encode(data interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	eh := EncodeHelper{Data: data}
	err := enc.Encode(eh)
	return buf.Bytes(), err
}

func decode(dbytes []byte) (interface{}, error) {
	buf := bytes.NewBuffer(dbytes)
	enc := gob.NewDecoder(buf)
	var eh EncodeHelper
	err := enc.Decode(&eh)
	return eh.Data, err
}

type FileStruct struct{
	Name string
    Version int64 
    Exptime time.Time
    Contents []byte
}

func (fs FileStruct) String() string{
    s := fmt.Sprintf("Name(%v) Version(%v) Exptime(%v) Contents(%v)", fs.Name, fs.Version, fs.Exptime, string(fs.Contents))
    return s
}

type Command struct{
	Cid int64
	Filestruct FileStruct
}

func (cmd Command) String() string{
	return fmt.Sprintf("Cid(%v) Filestruct(%v)", cmd.Cid, cmd.Filestruct)
}

var DebugRaft bool = false 
func debugRaft(str string){
	if DebugRaft{
		fmt.Println(str)
	}
}

type CommitInfo struct{
	Data []byte
	Index int
	Err error
}

type StateInfo struct {
	CurrTerm int
	VotedFor int
	Log      Log
}

type Message struct{
	OriginId int
	SendActn SendAction
}

type FsNode struct{
	appendCh chan Event
	commitCh chan *CommitInfo
	processQuitCh chan bool
	frontEndQuitCh chan bool
	backEndQuitCh chan bool
	isOn bool
	timer *time.Timer
	
	sm StateMachine
	smLock sync.RWMutex

	server cluster.Server

	mainLog *log.Log
	mainLogLock sync.RWMutex

	stateLog *log.Log
	stateLogLock sync.RWMutex

	stateIdx int

	pubaddr string
	socket *net.TCPListener
    fmLock sync.RWMutex
    dict map[string]*FileStruct
    cmLock sync.RWMutex
	connMap map[int64]*net.Conn
}


func registerStructs(){
	gob.Register(cluster.Envelope{})
	gob.Register(SendAction{})
	gob.Register(Message{})
	gob.Register(AppendEv{})
	gob.Register(AppendEntriesReqEv{})
	gob.Register(AppendEntriesRespEv{})
	gob.Register(VoteReqEv{})
	gob.Register(VoteRespEv{})
	gob.Register(Command{})
}

func NewFsNode(state State, id int,clusterConfigFileName string, logFileName string, hbTimeout int, timeout int, _pubaddr string) (*FsNode, error){
	//TODO Add code for recovery
	registerStructs()
	srvr,err  := cluster.New(id, clusterConfigFileName)
	if err != nil{
		return nil, err
	}

	_mainLog,err := log.Open(logFileName)
	if err != nil{
		return nil, err
	}

	_stateLog,err := log.Open(logFileName + "_state")
	if err != nil{
		return nil, err
	}

	_mainLog.RegisterSampleEntry([]byte{})
	_stateLog.RegisterSampleEntry(StateInfo{})

	_sm,alarm := NewSm(state, id, srvr.Peers(), hbTimeout, timeout)

	fsn := FsNode{appendCh:make(chan Event, 1000), commitCh:make(chan *CommitInfo, 1000), processQuitCh:make(chan bool, 1), frontEndQuitCh:make(chan bool, 1), backEndQuitCh:make(chan bool, 1), isOn:true, sm:_sm, server:srvr, mainLog:_mainLog, stateLog:_stateLog, dict:make(map[string]*FileStruct, 1000), pubaddr:_pubaddr, connMap:make(map[int64]*net.Conn)}
	fsn.timer = time.NewTimer(time.Millisecond * time.Duration(alarm.duration))

	addr, err := net.ResolveTCPAddr("tcp4", fsn.pubaddr)
	printErr(err)

	fsn.socket, err =  net.ListenTCP("tcp", addr)
	printErr(err)


	go fsn.frontEndMain()
	go fsn.backEndMain()
	go fsn.handleEvent()
	return &fsn, err
}

func NewMockFsNode(state State, srvr *mock.MockServer, logFileName string, hbTimeout int, timeout int, _pubaddr string) (*FsNode, error){
	//TODO Add code for recovery
    registerStructs()

	_mainLog,err := log.Open(logFileName)
	if err != nil{
		return nil, err
	}

	_stateLog,err := log.Open(logFileName + "_state")
	if err != nil{
		return nil, err
	}

	_mainLog.RegisterSampleEntry([]byte{})
	_stateLog.RegisterSampleEntry(StateInfo{})

	_sm,alarm := NewSm(state, srvr.Pid(), srvr.Peers(), hbTimeout, timeout)

	fsn := FsNode{appendCh:make(chan Event, 1000), commitCh:make(chan *CommitInfo, 1000), processQuitCh:make(chan bool),  frontEndQuitCh:make(chan bool, 1), backEndQuitCh:make(chan bool, 1), isOn:true, sm:_sm, server:srvr, mainLog:_mainLog, stateLog:_stateLog, dict:make(map[string]*FileStruct, 1000), pubaddr:_pubaddr, connMap:make(map[int64]*net.Conn)}
	fsn.timer = time.NewTimer(time.Millisecond * time.Duration(alarm.duration))


	addr, err := net.ResolveTCPAddr("tcp4", fsn.pubaddr)
	printErr(err)

	fsn.socket, err =  net.ListenTCP("tcp", addr)
	printErr(err)

	go fsn.frontEndMain()
	go fsn.backEndMain()
	go fsn.handleEvent()
	return &fsn, err
}

type Node interface {
	Append([]byte)

	CommitChannel() chan *CommitInfo

	CommitedIndex() int

	Get(index int) (err error, data []byte)

	Id()

	LeaderId() int

	Shutdown()
}

func (fsn *FsNode) Id() int{
	return fsn.server.Pid()
}

func (fsn *FsNode) LeaderId() int{
	return fsn.sm.leaderId
}


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
var redirerr = errors.New("ERR_REDIRECT") 

func (fsn *FsNode) parseCas(firstline string, conn net.Conn, reader *bufio.Reader) (*FileStruct, error){
    commands := strings.Fields(firstline)
    filename := commands[1]
    if len(commands) < 4 {
        debugServer("Compare and swap: insufficient arguments\n")
        fmt.Fprintf(conn, "%v \r\n", cmderr)
        return nil, nil
	}
    vers,err := strconv.ParseInt(commands[2], 10, 64)
    if err != nil {
		debugServer(fmt.Sprintf("Sending Internal Error: Version parsing error\n"))
        fmt.Fprintf(conn, "%v \r\n", cmderr)
        return nil, cmderr
    }

    numbytes,err := strconv.ParseInt(commands[3], 10, 64)
    if err != nil {
		debugServer(fmt.Sprintf("Sending Internal Error: Numbytes parsing error\n"))
        fmt.Fprintf(conn, "%v \r\n", cmderr)
        return nil,nil 
    }

    var exptime int64 = -1
    if len(commands) == 5 {
        exptime,err = strconv.ParseInt(commands[4], 10, 64)
        if err != nil {
			debugServer(fmt.Sprintf("Sending Internal Error: Exptime parsing error\n"))
            fmt.Fprintf(conn, "%v \r\n", cmderr)
            return nil, nil 
        }
    }

    var content = make([]byte,numbytes+2)
    n, ok := reader.Read(content)
    if ok != nil {
		debugServer(fmt.Sprintf("Sending Internal Error: Exptime parsing error\n"))
        fmt.Fprintf(conn, "%v \r\n", interr)
        conn.Close()
        return nil, interr
	}
    if len(content) != n{
        debugServer(fmt.Sprintf("Number of bytes %v not equal to length of content %v in CAS\n", n, len(content)))
    }
    fsn.fmLock.RLock()
    curr := fsn.dict[filename].Version
    if curr != vers {
        fsn.fmLock.RUnlock()
        fmt.Fprintf(conn, "%v %v \r\n",verserr, curr)
        return nil,nil 
    }
    fsn.fmLock.RUnlock()
	wfs := &FileStruct{Name:filename, Version:curr +1, Exptime:time.Now().Add(time.Duration(exptime)*time.Millisecond), Contents:content}
    return wfs, nil
}

func (fsn *FsNode) handleRead(firstline string, conn net.Conn) error{
    tokens := strings.Fields(firstline)
    filename := tokens[1]
    if len(tokens) != 2 {
        debugServer(fmt.Sprintf("Read: number of arguments:%d", len(tokens)))
        fmt.Fprintf(conn,"%v \r\n", cmderr)
        return nil 
    }
    fsn.fmLock.RLock()
    _, present := fsn.dict[filename] 
    if !present {
        fmt.Fprintf(conn,"%v \r\n", fnferr)
		fsn.fmLock.RUnlock()
        return nil 
    }
    vers :=  fsn.dict[filename].Version
    expirytime := fsn.dict[filename].Exptime
    fsn.fmLock.RUnlock()

    expired := !(expirytime.Sub(time.Now()) > time.Nanosecond)
    if expired {
        fmt.Fprintf(conn,"%v \r\n", fnferr)
        _ = os.Remove(filename)
        fsn.fmLock.Lock()
        delete(fsn.dict, filename)
        fsn.fmLock.Unlock()
        return nil 
    }

    fmt.Fprintf(conn,"CONTENTS %v %v %v  \r\n",vers, len(fsn.dict[filename].Contents)-2, expirytime)
    conn.Write(fsn.dict[filename].Contents)
    return nil
}

func (fsn *FsNode) parseWrite(firstline string, conn net.Conn, reader *bufio.Reader) (*FileStruct, error){
    tokens := strings.Fields(firstline)
    filename := tokens[1]
    if len(tokens) < 3 {
        debugServer(fmt.Sprintf("Read: number of arguments:%d", len(tokens)))
        fmt.Fprintf(conn,"%v \r\n", cmderr)
        return nil, nil
    }
    numbytes,err := strconv.ParseInt(tokens[2], 10, 64)
    if err != nil {
        fmt.Fprintf(conn,"%v \r\n", cmderr)
        return nil, nil 
    }
    var expirytime int64 = -1
    if len(tokens) == 4 {
        expirytime,err = strconv.ParseInt(tokens[3], 10, 64)
        if err != nil {
            fmt.Fprintf(conn,"%v \r\n", cmderr)
            return nil, nil 
        }
    }
    var content = make([]byte,numbytes+2)
    n, ok := reader.Read(content)
    if ok != nil {
        fmt.Fprintf(conn, "%v \r\n", interr)
        conn.Close()
        return nil, interr
	}
    if len(content) != n{
        debugServer(fmt.Sprintf("Number of bytes %v not equal to length of content %v in Write\n", n, len(content)))
    }
	debugServer(fmt.Sprintf("In write: contents received:%v\n", string(content)))
    fsn.fmLock.Lock()
    var vers int64 = 0
	_, present := fsn.dict[filename]
    if present {
        vers = fsn.dict[filename].Version + 1
    }
    fsn.fmLock.Unlock()
	wfs := &FileStruct{Name:filename, Contents:content, Exptime:time.Now().Add(time.Duration(expirytime)*time.Millisecond), Version:vers}
	debugServer("Write Returning")
    return wfs, nil
}

func (fsn *FsNode) parseDelete(firstline string, conn net.Conn) (*FileStruct, error){
	tokens := strings.Fields(firstline)
	filename := tokens[1]
	if len(tokens) != 2 {
		debugServer(fmt.Sprintf("Read: number of arguments:%d", len(tokens)))
		fmt.Fprintf(conn,"%v \r\n", cmderr)
		conn.Close()
		return nil, cmderr
	}
	return &FileStruct{Name:filename, Version:-1, Contents:[]byte{}}, nil
}

func (fsn *FsNode) handleSocket(conn net.Conn, cid int64){
	reader := bufio.NewReader(conn)
	for{
		buf, err := reader.ReadBytes('\n')
		if err != nil{
			fmt.Fprintf(conn, "ERR_INTERNAL \r\n")
			debugServer(fmt.Sprintf("handleSocket:Error in reading firstline\n"))
			conn.Close()
			return
		}

		firstline := string(buf)
		commands := strings.Fields(firstline)
		//fmt.Println(commands, len(commands))
		command := commands[0]
		if len(commands) < 2 {
			fmt.Fprintf(conn, "ERR_CMD_ERR \r\n")
			debugServer(fmt.Sprintf("handleSocket:len of commands:%v\n", len(commands)))
			conn.Close()
			return
		}
		debugServer(fmt.Sprintf("command:%v\n", command))
		
		switch command {
			case "cas": {
				wfs, err := fsn.parseCas(firstline, conn, reader)
				if wfs != nil{
					cmd := Command{Cid:cid, Filestruct:*wfs}
					cbytes, err := encode(cmd)
					if err != nil{
						fmt.Fprintf(conn, "%v\r\n", interr)
						conn.Close()
						debugServer(fmt.Sprintf("handleSocket:switch(cas)\n"))
						return
					}
					redirerr := fsn.Append(cbytes)
					if redirerr != nil{
						fmt.Fprintf(conn, "%v\r\n", redirerr)
						debugServer(fmt.Sprintf("handleSocket:switch(cas) redirection(%v)\n", redirerr))
						conn.Close()
						return 
					}
					debugServer(fmt.Sprintf("Cas: Appended Command:%v\n", cmd))
				}
				if err != nil{
					debugServer(fmt.Sprintf("handleSocket Returning: Cas\n"))
					return
				}
            }
			case "read": {
				err := fsn.handleRead(firstline, conn)
				if err != nil{
					return
				}
			}
			case "write": {
				wfs, err := fsn.parseWrite(firstline, conn, reader)
				debugServer(fmt.Sprintf("Back to handleSocket from Write %v\n", *wfs))
				if wfs != nil{
					cmd := Command{Cid:cid, Filestruct:*wfs}
					cbytes, err := encode(cmd)
					if err != nil{
						fmt.Fprintf(conn, "%v\r\n", interr)
						debugServer(fmt.Sprintf("handleSocket:switch(cas) err(%v)\n", interr))
						conn.Close()
						return
					}
					redirerr := fsn.Append(cbytes)
					debugServer("Message Appended:handleSocket(Write)\n")
					if redirerr != nil{
						debugServer(fmt.Sprintf("handleSocket:switch(cas) redirection(%v)\n", redirerr))
						conn.Close()
						return 
					}
				}
				if err != nil{
					debugServer(fmt.Sprintf("handleSocket Returning: Cas\n"))
					return
				}
			}
			case "delete": {
				wfs, err := fsn.parseDelete(firstline, conn)
				if wfs != nil{
					cmd := Command{Cid:cid, Filestruct:*wfs}
					cbytes, err := encode(cmd)
					if err != nil{
						fmt.Fprintf(conn, "%v\r\n", interr)
						conn.Close()
						return
					}
					redirerr := fsn.Append(cbytes)
					if redirerr != nil{
						fmt.Fprintf(conn, "%v\r\n", redirerr)
						conn.Close()
						return 
					}
				}
				if err != nil{
					debugServer(fmt.Sprintf("handleSocket Returning: Cas\n"))
					return
				}
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

func (fsn *FsNode) frontEndMain() {
	var cid int64 = 0
	for ;;cid++{
		select{
			case <- fsn.frontEndQuitCh:
				fsn.socket.Close()
				close(fsn.frontEndQuitCh)
				return
			default:
				conn, err := fsn.socket.Accept()
				if err != nil {
					continue
				}
				fsn.cmLock.Lock()
				fsn.connMap[cid] = &conn
				fsn.cmLock.Unlock()
				go fsn.handleSocket(conn, cid)
		}
	}
}

func (fsn *FsNode) CommitChannel() chan *CommitInfo{
	return fsn.commitCh
}

func (fsn *FsNode) Append(_data []byte) error{
	rid := fsn.Id()
	fsn.smLock.RLock()
	defer fsn.smLock.RUnlock()
	ldrId := fsn.LeaderId()
	if rid == ldrId{
        fsn.appendCh <- AppendEv{Data: _data}
		return nil
	}else{
		return errors.New(fmt.Sprintf("%v %v",redirerr, ldrId))	
	}
}

func (fsn *FsNode) Shutdown(){
	debugServer(fmt.Sprintf("NodeId:%d In shutdown\n", fsn.Id()))
	fsn.processQuitCh <- true
}

func (fsn *FsNode) doActions(actions []Action) {
	rid := fsn.Id()
	for _, action := range actions{
		if !fsn.isOn {
			break
		}
		switch action.(type){
		case SendAction:
			actObj := action.(SendAction)
			msg := Message{OriginId:rid, SendActn:actObj}
			fsn.server.Outbox() <- &cluster.Envelope{Pid:actObj.PeerId, MsgId:2, Msg:msg}
		case CommitAction:
			actObj := action.(CommitAction)
			fsn.commitCh <- &CommitInfo{Index:actObj.index, Data:actObj.data, Err:actObj.err}
		case LogStoreAction:
			actObj := action.(LogStoreAction)
			fsn.mainLogLock.Lock()
			fsn.mainLog.Append(actObj.data)
			fsn.mainLogLock.Unlock()
		case StateStoreAction:
			actObj := action.(StateStoreAction)
			fsn.stateLogLock.Lock()
			fsn.stateLog.TruncateToEnd(0)
			fsn.stateLog.Append(StateInfo{CurrTerm:actObj.currTerm, VotedFor:actObj.votedFor,  Log:actObj.log})
			fsn.stateLogLock.Unlock()
		case AlarmAction:
			actObj := action.(AlarmAction)
			set := fsn.timer.Reset(time.Duration(actObj.duration)* time.Millisecond)
			if !set{
				fsn.timer = time.NewTimer(time.Duration(actObj.duration)* time.Millisecond)
			}
		}
	}
}

func (fsn *FsNode) handleEvent() {
	rid := fsn.Id()
	for {
		select {
			case  <- fsn.timer.C:
				debugRaft(fmt.Sprintf("NodeId:%v -------------handleEvent(timeout) 1\n",rid))
				fsn.smLock.Lock()
				actions := fsn.sm.processEvent(TimeoutEv{})
				fsn.smLock.Unlock()
				fsn.doActions(actions)
			case ev := <- fsn.appendCh:
				debugServer(fmt.Sprintf("NodeId:%v -------------handleEvent(%v) 1\n",rid, ev))
				fsn.smLock.Lock()
				actions := fsn.sm.processEvent(ev)
				fsn.smLock.Unlock()
				fsn.doActions(actions)
			case env := <- fsn.server.Inbox():
				msgObj :=  env.Msg.(Message)
				debugRaft(fmt.Sprintf("NodeId:%v -------------handleEvent(%v)\n",rid, msgObj.SendActn.Event))
				fsn.smLock.Lock()
				actions := fsn.sm.processEvent(msgObj.SendActn.Event)
				fsn.smLock.Unlock()
				fsn.doActions(actions)
			case <- fsn.processQuitCh:
				debugServer(fmt.Sprintf("NodeId:%d In processQuitCh\n", fsn.Id()))
				fsn.frontEndQuitCh <- true
				fsn.backEndQuitCh <- true
				fsn.timer.Stop()
				fsn.server.Close()
				time.Sleep(1*time.Second)
				fsn.isOn = false
				close(fsn.processQuitCh)
				close(fsn.appendCh)
				fsn.mainLog.Close()
				fsn.stateLog.Close()
				debugServer(fmt.Sprintf("NodeId:%d handleEvent returned\n", fsn.Id()))
				return
		}
	}
}

func (fsn *FsNode) backEndMain(){
	for {
		select {
		case ci := <-fsn.CommitChannel():
			debugServer(fmt.Sprintf("In backendmain:%v\n", fsn.Id()))
			if ci.Err != nil {
				fmt.Fprintln(os.Stderr,ci.Err)
			}
			iface, err := decode(ci.Data)
			cmd := iface.(Command)
			if err != nil{
				panic(err)
			}
			fsn.cmLock.RLock()
			conn, present := fsn.connMap[cmd.Cid]
			if !present{
				fsn.cmLock.RUnlock()
				continue
			}
			fsn.cmLock.RUnlock()
			filestruct := cmd.Filestruct
			debugServer(fmt.Sprintf("backEndMain:%v\n", cmd))
			if filestruct.Version == -1 {
				fsn.fmLock.Lock()
				delete(fsn.dict, filestruct.Name)
				fsn.fmLock.Unlock()

				fmt.Fprintf(*conn, "OK\r\n")
			}else{
				fsn.fmLock.Lock()
				fsn.dict[filestruct.Name] = &filestruct 
				fsn.fmLock.Unlock()

				fmt.Fprintf(*conn, "OK %v\r\n", filestruct.Version)
			}
		case <- fsn.backEndQuitCh:
			for _, conn := range fsn.connMap{
				(*conn).Close()
			}
		}
	}
}
