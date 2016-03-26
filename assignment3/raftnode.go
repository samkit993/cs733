package main
import (
	"fmt"
	"time"
	"os"
	"sync"
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/cluster/mock"
	"github.com/cs733-iitb/log"
)


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
	SendAct Send
}

type RaftNode struct{
	sync.Mutex
	eventCh chan Event
	commitCh chan *CommitInfo
	listenQuitCh chan bool
	processQuitCh chan bool
	timer *time.Timer
	sm StateMachine
	server cluster.Server
	mainLog *log.Log
	stateLog *log.Log
	stateIdx int
}

func NewRN(state State, id int,clusterConfigFileName string, logFileName string, hbTimeout int, timeout int) (*RaftNode, error){
    os.RemoveAll(logFileName)
    os.RemoveAll(logFileName + "_state")
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

	rn := RaftNode{eventCh:make(chan Event, 1000), commitCh:make(chan *CommitInfo, 1000), listenQuitCh:make(chan bool), processQuitCh:make(chan bool), sm:_sm, server:srvr, mainLog:_mainLog, stateLog:_stateLog}
	rn.timer = time.NewTimer(time.Millisecond * time.Duration(alarm.duration))

	go rn.Listen()
	go rn.processEvents()
	return &rn, err
}

func NewMockRN(state State, srvr *mock.MockServer, logFileName string, hbTimeout int, timeout int) (*RaftNode, error){
    os.RemoveAll(logFileName)
    os.RemoveAll(logFileName + "_state")

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

	rn := RaftNode{eventCh:make(chan Event, 1000), commitCh:make(chan *CommitInfo, 1000), listenQuitCh:make(chan bool), processQuitCh:make(chan bool), sm:_sm, server:srvr, mainLog:_mainLog, stateLog:_stateLog}
	rn.timer = time.NewTimer(time.Millisecond * time.Duration(alarm.duration))

	go rn.Listen()
	go rn.processEvents()
	return &rn, err
}

type Node interface {
	Append([]byte)

	//A channel for client to listen on. What goes into Append must come out of here at some point
	CommitChannel() chan *CommitInfo

	//Last known committed index in the log. This could be -1 untill the system stabilizes
	CommitedIndex() int

	//Returns the data at a log index or an error
	Get(index int) (err error, data []byte)

	//Node's id
	Id()

	//Id of leader,-1 if unknown
	LeaderId() int

	//Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers
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
	if rn.Id() == rn.LeaderId(){
        rn.eventCh <- AppendEv{Data: _data}
	}else{
		//Send this append request to leader
		ldrId := rn.LeaderId()	
		if ldrId == -1{
			//CHECK 
			debug(fmt.Sprintf("%%%%%%%%%%%%%%%%%%%%%%%%%%Append Returning"))
			return
		}
		msg := Message{OriginId:rn.Id(),  SendAct:Send{PeerId:ldrId, Event:AppendEv{Data:_data}}}
		debug(fmt.Sprintf("%%%%%%%%%%%%%%%%%%%%%%%%%%Append Sending to %v", ldrId))
		rn.server.Outbox() <- &cluster.Envelope{Pid:ldrId, MsgId:1, Msg:msg}
	}
}

/* Shuts down raft cleanly
 *   Stops listen go routine and then waits for 10 ms to get existing events processed
 *   Then stops processEvent go routine and closes all three channels
 *   Atlast, call close method of logs and servers
 */
func (rn *RaftNode) Shutdown(){
	rn.timer.Stop()
	rn.listenQuitCh <- true
	time.Sleep(10*time.Millisecond)		//Wait, so that all messages are processed 
	rn.processQuitCh <- true
	close(rn.eventCh)
	close(rn.listenQuitCh)
	close(rn.processQuitCh)
	rn.mainLog.Close()
	rn.stateLog.Close()
	rn.server.Close()
}

func (rn *RaftNode) doActions(actions []Action) {
	for _, action := range actions{
		debug(fmt.Sprintf("Node Id(%v) leaderId(%v) State(%v)++++++++++++++++doActions(%v)\n",rn.Id(),rn.LeaderId(), rn.sm.state, action))
		switch action.(type){
		case Send:
			actObj := action.(Send)
			msg := Message{OriginId:rn.Id(), SendAct:actObj}
			rn.server.Outbox() <- &cluster.Envelope{Pid:actObj.PeerId, MsgId:2, Msg:msg}
		case Commit:
			actObj := action.(Commit)
			rn.commitCh <- &CommitInfo{Index:actObj.index, Data:actObj.data, Err:actObj.err}
		case LogStore:
			actObj := action.(LogStore)
			li := int(rn.mainLog.GetLastIndex())
			if li == actObj.index - 2{			//In raft log index starts at 1
				rn.mainLog.Append(actObj.data)
			}else if(li < actObj.index - 2){
				//return error
			}else{
				err := rn.mainLog.TruncateToEnd(int64(actObj.index-2))
				err = rn.mainLog.Append(actObj.data)
				if err != nil {
					panic(err)
				}
			}
		case StateStore:
			actObj := action.(StateStore)
			rn.stateLog.TruncateToEnd(0)
			err := rn.stateLog.Append(StateInfo{CurrTerm:actObj.currTerm, VotedFor:actObj.votedFor,  Log:actObj.log})
			if err != nil {
				panic(err)
			}
		case Alarm:
			actObj := action.(Alarm)
			set := rn.timer.Reset(time.Duration(actObj.duration)* time.Millisecond)
			if !set{
				rn.timer = time.NewTimer(time.Duration(actObj.duration)* time.Millisecond)
			}
		}
	}
}

func (rn *RaftNode) Listen(){
	for {
		select{
			case env := <- rn.server.Inbox():
				//fmt.Printf("Node Id(%v) State(%v) Listen.ReceivedMsg(%v)\n",rn.Id(), rn.sm.state, env.Msg)
				msgObj :=  env.Msg.(Message)
				rn.eventCh <- msgObj.SendAct.Event 
			case <- rn.listenQuitCh:
				return
		}
	}
}

func (rn *RaftNode) processEvents() {
	for {
		select {
			case <- rn.timer.C:
				debug(fmt.Sprintf("Node Id(%v) leaderId(%v) State(%v)-------------processEvents(timeout)\n",rn.Id(), rn.LeaderId(), rn.sm.state))
				actions := rn.sm.processEvent(TimeoutEv{})
				rn.doActions(actions)
			case ev := <- rn.eventCh:
				debug(fmt.Sprintf("Node Id(%v) leaderId(%v) State(%v)-------------processEvents(%v)\n",rn.Id(), rn.LeaderId(), rn.sm.state, ev))
				actions := rn.sm.processEvent(ev)
				rn.doActions(actions)
			case <- rn.processQuitCh:
				return
		}
	}
}

/*
func main(){
}
*/
