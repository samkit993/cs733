package main
import (
	"fmt"
	"time"
	"sync"
    "os"
	"encoding/gob"
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/cluster/mock"
	"github.com/cs733-iitb/log"
)

var DebugRaft bool = false 
func debugRaft(str string){
	if DebugRaft{
		fmt.Fprintf(os.Stderr, "!!!!!!!!!%v\n",str)
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

type RaftNode struct{
	appendCh chan Event
	commitCh chan *CommitInfo
	processQuitCh chan bool
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
}

func NewRN(state State, id int,clusterConfigFileName string, logFileName string, hbTimeout int, timeout int) (*RaftNode, error){
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

	rn := RaftNode{appendCh:make(chan Event, 1000), commitCh:make(chan *CommitInfo, 1000), processQuitCh:make(chan bool, 1),isOn:true, sm:_sm, server:srvr, mainLog:_mainLog, stateLog:_stateLog}
	rn.timer = time.NewTimer(time.Millisecond * time.Duration(alarm.duration))

	go rn.handleEvent()
	return &rn, err
}

func NewMockRN(state State, srvr *mock.MockServer, logFileName string, hbTimeout int, timeout int) (*RaftNode, error){
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

	rn := RaftNode{appendCh:make(chan Event, 1000), commitCh:make(chan *CommitInfo, 1000), processQuitCh:make(chan bool),isOn:true, sm:_sm, server:srvr, mainLog:_mainLog, stateLog:_stateLog}
	rn.timer = time.NewTimer(time.Millisecond * time.Duration(alarm.duration))

	go rn.handleEvent()
	return &rn, err
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

func (rn *RaftNode) Id() int{
	return rn.server.Pid()
}

func (rn *RaftNode) LeaderId() int{
	return rn.sm.leaderId
}

func (rn *RaftNode) CommitChannel() chan *CommitInfo{
	return rn.commitCh
}

func (rn *RaftNode) Append(_data []byte){
	rid := rn.Id()
	rn.smLock.RLock()
	defer rn.smLock.RUnlock()
	if rid == rn.LeaderId(){
        rn.appendCh <- AppendEv{Data: _data}
	}else{
		//Send this append request to leader
		ldrId := rn.LeaderId()	
		if ldrId == -1{
			//CHECK 
			debugRaft(fmt.Sprintf("%%%%%%%%%%%%%%%%%%%%%%%%%%Append Returning"))
			return
		}
		msg := Message{OriginId:rid,  SendActn:SendAction{PeerId:ldrId, Event:AppendEv{Data:_data}}}
		debugRaft(fmt.Sprintf("%%%%%%%%%%%%%%%%%%%%%%%%%%Append Sending to %v", ldrId))
		rn.server.Outbox() <- &cluster.Envelope{Pid:ldrId, MsgId:1, Msg:msg}
	}
}

/*   Shuts down raftnode cleanly
 */
func (rn *RaftNode) Shutdown(){
	rn.processQuitCh <- true
}

func (rn *RaftNode) doActions(actions []Action) {
	rid := rn.Id()
	for _, action := range actions{
		if !rn.isOn {
			break
		}
		switch action.(type){
		case SendAction:
			actObj := action.(SendAction)
			msg := Message{OriginId:rid, SendActn:actObj}
			rn.server.Outbox() <- &cluster.Envelope{Pid:actObj.PeerId, MsgId:2, Msg:msg}
		case CommitAction:
			actObj := action.(CommitAction)
			rn.commitCh <- &CommitInfo{Index:actObj.index, Data:actObj.data, Err:actObj.err}
		case LogStoreAction:
			actObj := action.(LogStoreAction)
			rn.mainLogLock.Lock()
			defer rn.mainLogLock.Unlock()
			li := int(rn.mainLog.GetLastIndex())
			if li == actObj.index - 2{			//In raft statemachine log index starts at 1
				//debugRaft(fmt.Sprintf("LogStoreAction:\"if \" NodeId:%v mainLogIndex:%v stateMachineIndex-2:%v\n",rid, li, actObj.index-2))
				rn.mainLog.Append(actObj.data)
			}else if(li < actObj.index - 2){
				//debugRaft(fmt.Sprintf("LogStoreAction: \"else if \" NodeId:%v mainLogIndex:%v stateMachineIndex-2:%v\n",rid, li, actObj.index-2))
                os.Exit(1)
			}else{	//Actual Log is greater than statmachine log
				//debugRaft(fmt.Sprintf("LogStoreAction: \"else\" NodeId:%v mainLogIndex:%v stateMachineIndex-2:%v\n",rid, li, actObj.index-2))
				err := rn.mainLog.TruncateToEnd(int64(actObj.index-2))
				err = rn.mainLog.Append(actObj.data)
				if err != nil {
					panic(err)
				}
			}
		case StateStoreAction:
			actObj := action.(StateStoreAction)
			rn.stateLogLock.Lock()
			rn.stateLog.TruncateToEnd(0)
			rn.stateLog.Append(StateInfo{CurrTerm:actObj.currTerm, VotedFor:actObj.votedFor,  Log:actObj.log})
			rn.stateLogLock.Unlock()
		case AlarmAction:
			actObj := action.(AlarmAction)
			set := rn.timer.Reset(time.Duration(actObj.duration)* time.Millisecond)
			if !set{
				rn.timer = time.NewTimer(time.Duration(actObj.duration)* time.Millisecond)
			}
		}
	}
}

func (rn *RaftNode) handleEvent() {
	rid := rn.Id()
	for {
		select {
			case  <- rn.timer.C:
				debugRaft(fmt.Sprintf("NodeId:%v State(%v)-------------handleEvent(timeout) 1\n",rid, rn.sm.state))
				rn.smLock.Lock()
				actions := rn.sm.processEvent(TimeoutEv{})
				rn.smLock.Unlock()
				rn.doActions(actions)
			case ev := <- rn.appendCh:
				debugRaft(fmt.Sprintf("NodeId:%v State(%v)-------------handleEvent(%v) 1\n",rid, rn.sm.state, ev))
				rn.smLock.Lock()
				actions := rn.sm.processEvent(ev)
				rn.smLock.Unlock()
				rn.doActions(actions)
			case env := <- rn.server.Inbox():
				msgObj :=  env.Msg.(Message)
				debugRaft(fmt.Sprintf("NodeId:%v State(%v)-------------handleEvent(%v)\n",rid,rn.sm.state, msgObj.SendActn.Event))
				rn.smLock.Lock()
				actions := rn.sm.processEvent(msgObj.SendActn.Event)
				rn.smLock.Unlock()
				rn.doActions(actions)
			case <- rn.processQuitCh:
				rn.timer.Stop()
				rn.server.Close()
				time.Sleep(1*time.Second)
				rn.isOn = false
				close(rn.processQuitCh)
				close(rn.appendCh)
				rn.mainLog.Close()
				rn.stateLog.Close()
				debugRaft(fmt.Sprintf("NodeId:%d handleEvent returned\n", rn.Id()))
				return
		}
	}
}

/*
func main(){
}
*/
