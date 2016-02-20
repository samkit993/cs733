package main
import (
	"fmt"
	"sort"
	"math/rand"
	"time"
)
/*
-	Currently this contains incomplete implementation for state machine. This is done for first check point instead of writing pseudcode. Might contain lot of typos as well.

-	Data structures used:
		LogEntry: 	For storing term and data pair in log
		EntryInfo:	Stores additional commit information about log entry

-	Following variables are used in StateMachine struct in addition to state variables of specification
		currState int				//0: Follower	1:Candidate		2:Leader
		votes int					//Relevant to candidate state
		logInfo map[int]EntryInfo	//logIndex:logEntryInfo is relevant to leader state
		timeOutPeriod int			//In miliseconds

-	TODO:
		1.	Need to define Msg and Action struct properly
		2.	Need to figure out where to place fail case of Commit outputcase
		3.	Clueless about where to place Alarm output action
		4.	And lot of others small technicalities/doubts need to be resolved
		5.	Main event loop and channel mechanism
*/
type State int

const (
	FOLLOWER State = iota
	CANDIDATE State = iota
	LEADER State = iota
)

const (
	heartBeatTimeout = 10
	Timeout int = 20
)

type LogEntry struct{
	term int
	data []byte
}

type StateMachine struct{
	id int
	currTerm int
	votedFor int
	log []LogEntry
	commitIndex int
	lastApplied int
	nextIndex map[int]int
	matchIndex map[int]int
	lastSent map[int]int
	state State				//0: Follower	1:Candidate		2:Leader
	votes map[int]bool					//Relevant to candidate state
	peers []int
	majorityCount int
}

type Event interface {

}

//6 Events
type TimeoutEv struct{

}

type AppendEntriesReqEv struct {
	term int
	leaderId int
	prevLogIndex int
	prevLogTerm int
	entries []LogEntry
	leaderCommit int
}

type AppendEntriesRespEv struct {
	term int
	success bool
}

type VoteReqEv struct {
	term int
	candidateId int
	lastLogIndex int
	lastLogTerm int
}

type VoteRespEv struct {
	term int
	voteGranted bool
}

type AppendEv struct {
	data []byte
}
//4 Actions
type Action interface {

}
type Send struct {
	peerId int
	event Event
}

type Commit struct {
	index int
	data []byte
	err error
}
func (c Commit) String() string{
	return fmt.Sprintf("Commit:idx(%i) data(%v) error(%v)",c.index, c.data, c.err)
}

type Alarm struct {
	duration int		//Time.Miliseconds
}
func (a Alarm) String() string{
	return fmt.Sprintf("Alarm:duration(%i)",a.duration)
}

type LogStore struct {
	index int
	data []byte
}

func (l LogStore) String() string{
	return fmt.Sprintf("LogStore:idx(%i) data(%v)",l.index, l.data)
}
/*	This is required to communicate command across network from one node to another node.
 *	Most probably, it is not required in this assignment
 */
type Msg struct {
	fromId int
	toId int
	event Event
}

func debug(s string){
	if false{
		fmt.Println(s)
	}
}

func max(a, b int) int{
	if a > b {
		return a
	}else{
		return b
	}
}

func min(a, b int) int{
	if a > b {
		return b
	}else{
		return a
	}
}
func getCommitIndex(matchIndex map[int]int, majorityCount int) int{
	matchings := []int{}
	for _, v := range matchIndex {
		matchings = append(matchings, v)
	}
	sort.Ints(matchings)
	return matchings[majorityCount-1]
}

func NewSm(_state State, _id int, _peers []int) StateMachine{
	_majorityCount := len(_peers)/2 + 1
	var sm StateMachine
	switch _state{
	case FOLLOWER, CANDIDATE:
		sm = StateMachine{id:_id, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:0,lastApplied:0, nextIndex:make(map[int]int), matchIndex:make(map[int]int),lastSent:make(map[int]int), state:_state, votes:make(map[int]bool),peers:_peers, majorityCount:_majorityCount}
	case LEADER:
		sm = StateMachine{id:_id, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:0,lastApplied:0, nextIndex:make(map[int]int), matchIndex:make(map[int]int),lastSent:make(map[int]int), state:_state, votes:make(map[int]bool),peers:_peers, majorityCount:_majorityCount}
		for _, peerId := range sm.peers {
			sm.matchIndex[peerId] = 1
			sm.nextIndex[peerId] = 1
			sm.lastSent[peerId] = 0
		}
	}
	return sm
}

func (sm *StateMachine) changeToFollower() []Action{
	sm.state = FOLLOWER
	sm.votedFor = -1
	sm.votes = make(map[int]bool)
	sm.matchIndex = make(map[int]int)
	sm.nextIndex = make(map[int]int)
	sm.lastSent = make(map[int]int)
	action := Alarm{duration:Timeout}
	actions := []Action{}
	actions = append(actions, action)
	return actions
}

func (sm *StateMachine) startElection() []Action{
	sm.currTerm += 1
	sm.votedFor = sm.id
	sm.votes[sm.id] = true
	_lastLogIndex := len(sm.log)-1
	_lastLogTerm := sm.log[_lastLogIndex].term
	actions := []Action{}
	for _, peer :=  range sm.peers{
		action := Send{peerId:peer, event:VoteReqEv{term:sm.currTerm, candidateId:sm.id, lastLogIndex:_lastLogIndex,lastLogTerm: _lastLogTerm}}
		actions = append(actions, action)
	}
	return actions
}

func (sm *StateMachine) changeToCandidate() []Action{
	sm.state = CANDIDATE
	actions := sm.startElection()
	action := Alarm{duration:(Timeout + rand.Intn(Timeout))}
	actions = append(actions,action)
	return actions
}

func (sm *StateMachine) sendHeartBeats() []Action{
	entry := LogEntry{term:sm.currTerm, data:[]byte{}}
	_entries := []LogEntry{}
	_entries = append(_entries, entry)
	_prevLogIndex := len(sm.log)-1
	_prevLogTerm := sm.log[_prevLogIndex-1].term
	actions := []Action{}
	for _, _peerId :=  range sm.peers {
		action := Send{peerId:_peerId, event:AppendEntriesReqEv{term:sm.currTerm, leaderId:sm.id, prevLogIndex:_prevLogIndex,prevLogTerm:_prevLogTerm, entries:_entries, leaderCommit:sm.commitIndex}}			//Sending Heartbeat
		sm.lastSent[_peerId] = 0
		actions = append(actions, action)
	}
	action := Alarm{duration:heartBeatTimeout}
	actions = append(actions,action)
	return actions
}

func (sm *StateMachine) changeToLeader() []Action{
	sm.state = LEADER
	sm.votes = make(map[int]bool)
	sm.votedFor = -1
	sm.matchIndex = make(map[int]int)
	sm.nextIndex = make(map[int]int)
	sm.lastSent = make(map[int]int)
	for _, _peerId := range sm.peers {
		sm.matchIndex[_peerId] = 1
		sm.nextIndex[_peerId] = len(sm.log) + 1		//CHECK
		sm.lastSent[_peerId] = 0
	}
	actions := sm.sendHeartBeats()
	return actions
}

func (sm *StateMachine) handleTimeout() []Action{
	//If voted for no one then, become candidate
	actions := []Action{}
	switch sm.state{
		case FOLLOWER:
			if sm.votedFor == -1 {
				actions = sm.changeToCandidate()
			}
		case CANDIDATE:
			actions = sm.startElection()
		case LEADER:
			//Sending the heartbeat to everybody
			actions = sm.sendHeartBeats()
	}
	return actions
}

func (sm *StateMachine) handleAppendEntriesReqGeneric(fromId int,term int, leaderId int, prevLogIndex int, prevLogTerm int, entries []LogEntry, leaderCommit int) []Action{
	prevLogIndex -= 1 			//Log Index according to spec start at 1
	actions := []Action{}
	if term >= sm.currTerm && len(sm.log) > prevLogIndex && sm.log[prevLogIndex].term == prevLogTerm {
		if entries != nil {
			sm.currTerm = term                //Check
			for _,entry := range entries {
				if len(sm.log) - 1 < prevLogIndex {
					//append the entry to log
					sm.log = append(sm.log, entry)
				}else {
					//replace the entry in log
					sm.log[prevLogIndex + 1] = entry
				}
				sm.lastApplied = max(sm.lastApplied, prevLogIndex + 1)
				oldCommitIndex := sm.commitIndex
				if leaderCommit > sm.commitIndex {
					sm.commitIndex = min(leaderCommit, len(sm.log))
					//Accordingly LogStore action
					for ; oldCommitIndex < sm.commitIndex; {
						action := LogStore{index:oldCommitIndex - 1, data:sm.log[oldCommitIndex - 1].data}
						actions = append(actions, action)
						oldCommitIndex += 1
					}
				}
			}
		}
	}else{
		action := AppendEntriesRespEv{term:sm.currTerm, success:false}
		actions = append(actions, action)
	}
	return actions
}
func (sm *StateMachine) handleAppendEntriesReq(fromId int,term int, leaderId int, prevLogIndex int, prevLogTerm int, entries []LogEntry, leaderCommit int) []Action{
	actions := []Action{}
	switch sm.state{
		case FOLLOWER:
			actions = sm.handleAppendEntriesReqGeneric(fromId, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit)
		case CANDIDATE:
			if term > sm.currTerm {
				actions = append(actions, sm.changeToFollower()...)
			}
			actions = sm.handleAppendEntriesReqGeneric(fromId, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit)
		case LEADER:
			//Should not receive one, except when there are two leaders at the time of transition(addition/deletion of new node). Yet to handle later case
			//TODO
			if term > sm.currTerm {
				sm.changeToFollower()
				actions = sm.handleAppendEntriesReqGeneric(fromId, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit)
			}
	}
	return actions
}

func (sm *StateMachine) handleAppendEntriesResp(_peerId int, term int, success bool) []Action{
	actions := []Action{}
	switch sm.state{
		case FOLLOWER:
			//Will never get one ;-)
		case CANDIDATE:
			//Will never get one ;-)
		case LEADER:
			if success == false {
				sm.nextIndex[_peerId] -= sm.lastSent[_peerId]
				_prevLogIndex := sm.nextIndex[_peerId]-1
				_prevLogTerm := sm.log[_prevLogIndex-1].term	//'Coz log is initialized at 1 according to spec
				_entries := []LogEntry{sm.log[sm.nextIndex[_peerId]-1]}
				action := Send{peerId:_peerId, event:AppendEntriesReqEv{term:sm.currTerm, leaderId: sm.id,prevLogIndex:_prevLogIndex,prevLogTerm:_prevLogTerm, entries:_entries, leaderCommit:sm.commitIndex}}
				actions = append(actions, action)
			}else{
				sm.matchIndex[_peerId] += sm.lastSent[_peerId]
				newCommitIndex := getCommitIndex(sm.matchIndex, sm.majorityCount)
				if newCommitIndex > sm.commitIndex && sm.log[newCommitIndex-1].term == sm.currTerm {
					oldCommitIndex := sm.commitIndex
					sm.commitIndex = newCommitIndex
					for ;oldCommitIndex < sm.commitIndex; {
						action1 :=  Commit{index:oldCommitIndex, data:sm.log[oldCommitIndex-1].data, err:nil}
						actions = append(actions, action1)
						action2 := LogStore{index:oldCommitIndex, data:sm.log[oldCommitIndex-1].data}
						actions = append(actions, action2)
						oldCommitIndex += 1
					}
				}
				if sm.nextIndex[_peerId] <= sm.lastApplied {
					_prevLogIndex := sm.nextIndex[_peerId]-1
					_prevLogTerm := sm.log[_prevLogIndex-1].term	//'Coz log is initialized at 1 according to spec
					_entries := []LogEntry{}
					idx := sm.nextIndex[_peerId]
					for ;idx <= sm.lastApplied;{
						entry := LogEntry{term:sm.log[idx-1].term, data:sm.log[idx-1].data}	//Directly copy it
						_entries = append(_entries, entry)
						idx += 1
					}
					action := Send{_peerId, AppendEntriesReqEv{term:sm.currTerm, leaderId:sm.id,prevLogIndex:_prevLogIndex,prevLogTerm:_prevLogTerm, entries:_entries, leaderCommit:sm.commitIndex}}
					sm.lastSent[_peerId] = len(_entries)
					actions = append(actions, action)

				}
			}
	}
	return actions
}

func (sm *StateMachine) handleVoteReq(term int, candidateId int, lastLogIndex int,  lastLogTerm int) []Action{
	lastLogIndex -= 1 			//Log Index according to spec start at 1
	_voteGranted := false
	actions := []Action{}
	switch sm.state{
		case FOLLOWER:
			if term >= sm.currTerm {
				if sm.votedFor == -1 || sm.votedFor == candidateId {
					if term > sm.currTerm || ( sm.currTerm == term && lastLogIndex  > len(sm.log) ) {			//Check
						sm.votedFor = candidateId
						_voteGranted = true
					}
				}
			}
		case CANDIDATE:
			if term > sm.currTerm {
				sm.changeToFollower()
				sm.votedFor = candidateId
				_voteGranted = true
			}

		case LEADER:
			//Ignore
	}
	action := Send{peerId:candidateId, event:VoteRespEv{term:sm.currTerm, voteGranted:_voteGranted}}
	actions = append(actions, action)
	return actions
}

func (sm *StateMachine) handleVoteResp (fromId int, term int, voteGranted bool) []Action{
	//Will never get one ;-)
	actions := []Action{}
	switch sm.state{
		case FOLLOWER:
			//Ignore
		case CANDIDATE:
			if voteGranted {
				sm.votes[fromId] = true
			}
			if len(sm.votes) >= sm.majorityCount {
				//Upgrade to leader, send heartbeat to everyone
				actions = sm.changeToLeader()
			}
		case LEADER:
			//Ignore
	}
	return actions
}

func (sm *StateMachine) handleAppend(_data []byte) []Action{
	actions := []Action{}
	switch sm.state{
		case FOLLOWER:
			//Do nothing (or just call current leader's append implementation)
		case CANDIDATE:
			//Will never get one ?
		case LEADER:
			entry := LogEntry{term:sm.currTerm, data: _data}
			sm.log = append(sm.log, entry)
			lastLogIndex := len(sm.log)
			sm.lastApplied = lastLogIndex
			actions := []Action{}
			_entries := []LogEntry{}
			for _, _peerId :=  range sm.peers{
				if sm.nextIndex[_peerId] <= sm.lastApplied{
					_prevLogIndex := sm.nextIndex[_peerId]-1
					_prevLogTerm := sm.log[_prevLogIndex-1].term	//'Coz log is initialized at 1 according to spec
					idx := sm.nextIndex[_peerId]
					for ;idx <= lastLogIndex ; {
						entry := LogEntry{term:sm.log[idx-1].term, data:sm.log[idx-1].data}
						_entries = append(_entries, entry)
						idx += 1
					}
					action := Send{peerId:_peerId, event:AppendEntriesReqEv{term:sm.currTerm,leaderId:sm.id,prevLogIndex: _prevLogIndex,prevLogTerm:_prevLogTerm, entries:_entries, leaderCommit:sm.commitIndex}}
					sm.lastSent[_peerId] = len(_entries)
					actions = append(actions, action)
				}
			}
	}
	return actions
}
//fromId : 0 for client
func (sm *StateMachine) ProcessEvent (fromId int,ev Event) []Action{
	actions := []Action{}
	switch ev.(type) {
		case AppendEntriesReqEv:
			evObj := ev.(AppendEntriesReqEv)
			actions = sm.handleAppendEntriesReq(fromId, evObj.term, evObj.leaderId, evObj.prevLogIndex, evObj.prevLogTerm, evObj.entries, evObj.leaderCommit)
			debug(fmt.Sprintf("%v\n", evObj))
		case AppendEntriesRespEv:
			evObj := ev.(AppendEntriesRespEv)
			actions = sm.handleAppendEntriesResp(fromId, evObj.term, evObj.success)
			debug(fmt.Sprintf("%v\n", evObj))
		case VoteReqEv:
			evObj := ev.(VoteReqEv)
			actions = sm.handleVoteReq(evObj.term, evObj.candidateId, evObj.lastLogIndex, evObj.lastLogTerm)
			debug(fmt.Sprintf("%v\n", evObj))
		case VoteRespEv:
			evObj := ev.(VoteRespEv)
			actions = sm.handleVoteResp(fromId,evObj.term, evObj.voteGranted)
			debug(fmt.Sprintf("%v\n", evObj))
		case TimeoutEv:
			evObj := ev.(TimeoutEv)
			actions = sm.handleTimeout()
			debug(fmt.Sprintf("%v\n", evObj))
		case AppendEv:
			evObj := ev.(AppendEv)
			actions = sm.handleAppend(evObj.data)
			debug(fmt.Sprintf("%v\n", evObj))
		// other cases
		default:
			println ("Unrecognized")
	}
	return actions
}

func main(){
	rand.Seed(time.Now().UTC().UnixNano())
}
