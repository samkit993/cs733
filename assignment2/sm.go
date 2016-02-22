package assignment2
import (
	"fmt"
	"sort"
	"math/rand"
	"time"
)
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
type Log []LogEntry
type MapIntInt map[int]int
type IntArray []int
type MapIntBool map[int]bool

func (le LogEntry) String() string{
	return fmt.Sprintf("%v:%v", le.term, string(le.data))
}
func (log Log) String() string{
	str := "["
	for _, le := range log {
		str += fmt.Sprintf("%v:%v", le.term, string(le.data))
	}
	str += "]"
	return str
}

func (mapii MapIntInt) String() string{
	str := "[ "
	for key, val := range mapii{
		str += fmt.Sprintf("%v:%v ", key, val)
	}
	str += "]"
	return str
}

func (ia IntArray) String() string{
	str := "["
	for _, val := range ia{
		str += fmt.Sprintf("%v ", val)
	}
	str += "]"
	return str
}
func (mapib MapIntBool) String() string{
	str := "["
	for _, val := range mapib{
		str += fmt.Sprintf("%v ", val)
	}
	str += "]"
	return str
}

type StateMachine struct{
	id int
	currTerm int
	votedFor int
	log Log
	commitIndex int
	lastApplied int
	nextIndex MapIntInt
	matchIndex MapIntInt
	lastSent MapIntInt
	state State				//0: Follower	1:Candidate		2:Leader
	votes MapIntBool
	peers IntArray
	majorityCount int
}
func (sm *StateMachine) String() string{
	s := fmt.Sprintf("StateMachine:id(%v) currTerm(%v) votedFor(%v) state(%v)\n", sm.id, sm.currTerm, sm.votedFor, sm.state)
	s += fmt.Sprintf("            :commitIndex(%v) lastApplied(%v) majorityCount(%v)\n", sm.commitIndex, sm.lastApplied, sm.majorityCount)
	s += fmt.Sprintf("            :peers(%v) log(%v)\n", sm.peers, sm.log)
	s += fmt.Sprintf("            :nextIndex(%v) matchIndex(%v)\n", sm.nextIndex, sm.matchIndex)
	s += fmt.Sprintf("            :lastSent(%v) votes(%v)\n", sm.lastSent, sm.votes)
	return s
}

/*	This is required to communicate command across network from one node to another node.
 *	Most probably, it is not required in this assignment
 */
type Msg struct {
	originId int
	action Action
}
func (msg Msg) String() string{
	return fmt.Sprintf("Msg: originId(%v) action(%v)", msg.originId, msg.action.String())
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
	matchIndices := []int{}
	for _, v := range matchIndex {
		matchIndices = append(matchIndices, v)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(matchIndices)))		//StackOverflow, I love you
	return matchIndices[majorityCount-1]
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
			sm.matchIndex[peerId] = 0
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
//Only for FOLLOWER and CANDIDATE states
func (sm *StateMachine) updateCommitIndex(leaderCommit int) []Action{
	oldCommitIndex := sm.commitIndex
	actions := []Action{}
	if leaderCommit > sm.commitIndex {
		sm.commitIndex = min(leaderCommit, len(sm.log))
		//Accordingly LogStore action
		for ; oldCommitIndex < sm.commitIndex; {
			oldCommitIndex += 1
			action1 :=  Commit{index:oldCommitIndex, data:sm.log[oldCommitIndex-1].data, err:nil}
			actions = append(actions, action1)
			action2 := LogStore{index:oldCommitIndex, data:sm.log[oldCommitIndex-1].data}
			actions = append(actions, action2)
		}
	}
	return actions

}
func (sm *StateMachine) handleAppendEntriesReqGeneric(fromId int,term int, leaderId int, prevLogIndex int, prevLogTerm int, entries []LogEntry, leaderCommit int) []Action{
	prevLogIndex -= 1 			//Log Index according to spec start at 1
	actions := []Action{}
	if len(sm.log) == 0{
		for _,entry := range entries {
			sm.log = append(sm.log, entry)

		}
		sm.lastApplied = max(sm.lastApplied, len(sm.log))
		actions = sm.updateCommitIndex(leaderCommit)
		action := Send{peerId:fromId, event:AppendEntriesRespEv{term:sm.currTerm, success:true}}
		actions = append(actions, action)

	}else if term >= sm.currTerm && len(sm.log) > prevLogIndex && sm.log[prevLogIndex].term == prevLogTerm {
		if entries != nil {
			sm.currTerm = term                //Check
			for _,entry := range entries {
				if len(sm.log) - 1 <= prevLogIndex {
					//append the entry to log
					sm.log = append(sm.log, entry)
				}else {
					//replace the entry in log
					sm.log[prevLogIndex + 1] = entry
				}
				sm.lastApplied = max(sm.lastApplied, prevLogIndex + 1)
			}
		}
		actions = sm.updateCommitIndex(leaderCommit)
		action := Send{peerId:fromId, event:AppendEntriesRespEv{term:sm.currTerm, success:true}}
		actions = append(actions, action)
	}else{
		action := Send{peerId:fromId, event:AppendEntriesRespEv{term:sm.currTerm, success:false}}
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
			if term > sm.currTerm {
				sm.changeToFollower()
				actions = sm.handleAppendEntriesReqGeneric(fromId, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit)
			}
	}
	return actions
}

func (sm *StateMachine) handleAppendEntriesResp(fromId int, term int, success bool) []Action{
	actions := []Action{}
	switch sm.state{
		case FOLLOWER:
			//Will never get one ;-)
		case CANDIDATE:
			//Will never get one ;-)
		case LEADER:
			if success == false {
				sm.nextIndex[fromId] -= sm.lastSent[fromId]
				sm.lastSent[fromId] = 0
				_prevLogIndex := sm.nextIndex[fromId]-1
				_prevLogTerm := sm.log[_prevLogIndex-1].term	//'Coz log is initialized at 1 according to spec
				_entries := []LogEntry{sm.log[sm.nextIndex[fromId]-1]}
				action := Send{peerId:fromId, event:AppendEntriesReqEv{term:sm.currTerm, leaderId: sm.id,prevLogIndex:_prevLogIndex,prevLogTerm:_prevLogTerm, entries:_entries, leaderCommit:sm.commitIndex}}
				actions = append(actions, action)
			}else{
				sm.matchIndex[fromId] += sm.lastSent[fromId]
				sm.nextIndex[fromId] += sm.lastSent[fromId]
				sm.lastSent[fromId] = 0
				newCommitIndex := getCommitIndex(sm.matchIndex, sm.majorityCount)
				if newCommitIndex > sm.commitIndex && sm.log[newCommitIndex-1].term == sm.currTerm {
					oldCommitIndex := sm.commitIndex
					sm.commitIndex = newCommitIndex
					for ;oldCommitIndex < sm.commitIndex; {
						oldCommitIndex += 1
						action1 :=  Commit{index:oldCommitIndex, data:sm.log[oldCommitIndex-1].data, err:nil}
						actions = append(actions, action1)
						action2 := LogStore{index:oldCommitIndex, data:sm.log[oldCommitIndex-1].data}
						actions = append(actions, action2)
					}
				}
				if sm.nextIndex[fromId] <= sm.lastApplied {
					_prevLogIndex := sm.nextIndex[fromId]-1
					_prevLogTerm := sm.log[_prevLogIndex-1].term	//'Coz log is initialized at 1 according to spec
					_entries := []LogEntry{}
					idx := sm.nextIndex[fromId]
					for ;idx <= sm.lastApplied;{
						entry := LogEntry{term:sm.log[idx-1].term, data:sm.log[idx-1].data}	//Directly copy it
						_entries = append(_entries, entry)
						idx += 1
					}
					action := Send{fromId, AppendEntriesReqEv{term:sm.currTerm, leaderId:sm.id,prevLogIndex:_prevLogIndex,prevLogTerm:_prevLogTerm, entries:_entries, leaderCommit:sm.commitIndex}}
					sm.lastSent[fromId] = len(_entries)
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
			for _, _peerId :=  range sm.peers{
				_entries := []LogEntry{}
				if sm.nextIndex[_peerId] <= sm.lastApplied{
					_prevLogIndex := sm.nextIndex[_peerId]-1
					idx := sm.nextIndex[_peerId]
					for ;idx <= lastLogIndex ; {
						entry := LogEntry{term:sm.log[idx-1].term, data:sm.log[idx-1].data}
						_entries = append(_entries, entry)
						idx += 1
					}
					_prevLogTerm := 0
					if _prevLogIndex != 0 {
						_prevLogTerm = sm.log[_prevLogIndex-1].term
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
func (sm *StateMachine) processEvent (fromId int,ev Event) []Action{
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
	sm3 := NewSm(LEADER, 3, []int{1,2})
	actions := sm3.processEvent(0, AppendEv{data:[]byte("first")})
	fmt.Println(len(actions))
	for _, action := range actions {
		switch action.(type){
		case Send:
			actObj := action.(Send)
			fmt.Println(actObj.String())
		case Commit:
			actObj := action.(Commit)
			fmt.Println(actObj.String())
		case LogStore:
			actObj := action.(LogStore)
			fmt.Println(actObj.String())
		case Alarm:
			actObj := action.(LogStore)
			fmt.Println(actObj.String())
		}
	}
}

