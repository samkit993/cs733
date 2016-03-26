package main
import (
	"fmt"
	"sort"
	"bytes"
	"math/rand"
)
type State int

const (
	FOLLOWER State = iota
	CANDIDATE State = iota
	LEADER State = iota
)

type LogEntry struct{
	Term int
	Data []byte
}

type Update struct{
	Idx int
	Entry LogEntry
}

type Log []LogEntry
type LogUpdate []Update
type MapIntInt map[int]int
type IntArray []int
type MapIntBool map[int]bool

func (le LogEntry) String() string{
	return fmt.Sprintf("%v:%v", le.Term, string(le.Data))
}

func (update Update) String() string{
	return fmt.Sprintf("Idx(%v)LogEntry(%v:%v)", update.Idx,update.Entry.Term, string(update.Entry.Data))
}

func (log Log) String() string{
	str := "["
	for _, le := range log {
		str += fmt.Sprintf("%v:%v", le.Term, string(le.Data))
	}
	str += "]"
	return str
}

func (lu LogUpdate) String() string{
	str := ""
	for i, update := range lu {
		if i == len(lu)-1{
			str += fmt.Sprintf("%v", update)
		}else{
			str += fmt.Sprintf("%v,", update)
		}

	}
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
    leaderId int        //Currently known leader Id, -1 for no knowledge about it
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
	heartBeatTimeout int 
	timeout int 
}
func (sm *StateMachine) String() string{
	s := fmt.Sprintf("StateMachine:id(%v) leaderId(%v) currTerm(%v) votedFor(%v) state(%v)\n", sm.id, sm.leaderId, sm.currTerm, sm.votedFor, sm.state)
	s += fmt.Sprintf("            :commitIndex(%v) lastApplied(%v) majorityCount(%v)\n", sm.commitIndex, sm.lastApplied, sm.majorityCount)
	s += fmt.Sprintf("            :peers(%v) log(%v)\n", sm.peers, sm.log)
	s += fmt.Sprintf("            :nextIndex(%v) matchIndex(%v)\n", sm.nextIndex, sm.matchIndex)
	s += fmt.Sprintf("            :lastSent(%v) votes(%v)\n", sm.lastSent, sm.votes)
	s += fmt.Sprintf("            :heartBeatTimeout(%v) timeout(%v)\n", sm.heartBeatTimeout, sm.timeout)
	return s
}

/*	This is required to communicate command across network from one node to another node.
 *	Most probably, it is not required in this assignment
 */
type Msg struct {
	OriginId int
	Action   Action
}

func (msg Msg) String() string{
	return fmt.Sprintf("Msg: originId(%v) action(%v)", msg.OriginId, msg.Action.String())
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

func NewSm(_state State, _id int, _peers []int, hbTimeout int, _timeout int) (StateMachine, Alarm){
	_majorityCount := len(_peers)/2 + 1
	var sm StateMachine
	var alarm Alarm
	switch _state{
	case FOLLOWER, CANDIDATE:
		sm = StateMachine{id:_id, leaderId:-1, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:0,lastApplied:0, nextIndex:make(map[int]int), matchIndex:make(map[int]int),lastSent:make(map[int]int), state:_state, votes:make(map[int]bool),peers:_peers, majorityCount:_majorityCount, heartBeatTimeout:hbTimeout, timeout:_timeout}
		alarm = Alarm{duration:sm.timeout + rand.Intn(sm.timeout)}
	case LEADER:
        sm = StateMachine{id:_id, leaderId:_id, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:0,lastApplied:0, nextIndex:make(map[int]int), matchIndex:make(map[int]int),lastSent:make(map[int]int), state:_state, votes:make(map[int]bool),peers:_peers, majorityCount:_majorityCount, heartBeatTimeout:hbTimeout, timeout:_timeout}
		for _, peerId := range sm.peers {
			sm.matchIndex[peerId] = 0
			sm.nextIndex[peerId] = 1
			sm.lastSent[peerId] = 0
		}
		alarm = Alarm{duration:sm.heartBeatTimeout}
	}
	return sm, alarm
}

func (sm *StateMachine) changeToFollower() []Action{
	sm.state = FOLLOWER
	sm.votedFor = -1
	sm.votes = make(map[int]bool)
	sm.matchIndex = make(map[int]int)
	sm.nextIndex = make(map[int]int)
	sm.lastSent = make(map[int]int)
	action := Alarm{duration:sm.timeout}
	actions := []Action{}
	actions = append(actions, action)
	return actions
}

func (sm *StateMachine) startElection() []Action{
	sm.currTerm += 1
	sm.votedFor = sm.id
    sm.leaderId = -1
	sm.votes[sm.id] = true
	var _lastLogIndex, _lastLogTerm int
	if(len(sm.log) == 0){
		_lastLogIndex = 0
		_lastLogTerm = 0
	}else{
		_lastLogIndex = len(sm.log)
		_lastLogTerm = sm.log[_lastLogIndex-1].Term
	}
	actions := []Action{}
	for _, peer :=  range sm.peers{
		action := Send{PeerId:peer, Event:VoteReqEv{Term:sm.currTerm, CandidateId:sm.id, LastLogIndex:_lastLogIndex, LastLogTerm: _lastLogTerm}}
		actions = append(actions, action)
	}
	action := StateStore{currTerm:sm.currTerm, votedFor:sm.votedFor, log:sm.log}
	actions = append(actions, action)
	action1 := Alarm{duration:(sm.timeout + rand.Intn(sm.timeout))}
	actions = append(actions,action1)
	return actions
}

func (sm *StateMachine) changeToCandidate() []Action{
	debug(fmt.Sprintf("CALLED\n"))
	sm.state = CANDIDATE
	actions := sm.startElection()
	return actions
}

func (sm *StateMachine) sendHeartBeats() []Action{
	update := Update{Idx:-1,Entry:LogEntry{Term:sm.currTerm, Data:[]byte{}}}
	_updates := []Update{}
	_updates = append(_updates, update)
	var _prevLogIndex, _prevLogTerm int
	if(len(sm.log) == 0){
		_prevLogIndex = 0
		_prevLogTerm = 0
	}else{
		_prevLogIndex = len(sm.log)
		_prevLogTerm = sm.log[_prevLogIndex-1].Term
	}
	actions := []Action{}
	for _, _peerId :=  range sm.peers {
        action := Send{PeerId:_peerId, Event:AppendEntriesReqEv{FromId:sm.id, Term:sm.currTerm, LeaderId:sm.id, PrevLogIndex:_prevLogIndex, PrevLogTerm:_prevLogTerm, Updates:_updates, LeaderCommit:sm.commitIndex}}			//Sending Heartbeat
		//sm.lastSent[_peerId] = 0						//This is not required. Because, it might happen that before receiving resp for previous actual append, leader times out. In that case, it would overwrite the actual lastSent and would lead to not updation of matchIndex, nextIndex and commitIndex
		actions = append(actions, action)
	}
	action := Alarm{duration:sm.heartBeatTimeout}
	actions = append(actions,action)
	return actions
}

func (sm *StateMachine) changeToLeader() []Action{
	sm.state = LEADER
	sm.votes = make(map[int]bool)
	sm.votedFor = -1
    sm.leaderId = sm.id
	sm.matchIndex = make(map[int]int)
	sm.nextIndex = make(map[int]int)
	sm.lastSent = make(map[int]int)
	for _, _peerId := range sm.peers {
		sm.matchIndex[_peerId] = 0
		sm.nextIndex[_peerId] = len(sm.log) + 1		//CHECK
		sm.lastSent[_peerId] = 0
	}
	actions := sm.sendHeartBeats()
	action := StateStore{currTerm:sm.currTerm, votedFor:sm.votedFor, log:sm.log}
	actions = append(actions, action)
	return actions
}

func (sm *StateMachine) handleTimeout() []Action{
	actions := []Action{}
	debug(fmt.Sprintf("Node Id:%d State:%d LeaderId:%d\n", sm.id, sm.state, sm.leaderId))
	switch sm.state{
        //If voted for noone then, become candidate
		case FOLLOWER:
			if sm.votedFor == -1 {
				actions = sm.changeToCandidate()
			}else{
				sm.votedFor = -1
			}
		case CANDIDATE:
			actions = sm.startElection()
		case LEADER:
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
			action1 :=  Commit{index:oldCommitIndex, data:sm.log[oldCommitIndex-1].Data, err:nil}
			actions = append(actions, action1)
			action2 := LogStore{index:oldCommitIndex, data:sm.log[oldCommitIndex-1].Data}
			actions = append(actions, action2)
		}
	}
	return actions
}

func (sm *StateMachine) handleAppendEntriesReqGeneric(fromId int,term int, leaderId int, prevLogIndex int, prevLogTerm int, updates []Update, leaderCommit int) []Action{
	prevLogIndex -= 1 			//Log Index according to spec start at 1
	actions := []Action{}
	//fmt.Printf("term(%v) sm.currTerm(%v) len(sm.log)(%v) prevLogIndex(%v) 
	if term >= sm.currTerm && (len(sm.log) == 0 || (prevLogIndex == -1 && prevLogTerm == 0) || (len(sm.log) > prevLogIndex && sm.log[prevLogIndex].Term == prevLogTerm)) {
		sm.currTerm = term                //Check
		for _, update := range updates {
			if(update.Idx == -1) {
				continue
			}
			if(update.Idx <= len(sm.log)){
				if sm.log[update.Idx-1].Term == update.Entry.Term && bytes.Equal(sm.log[update.Idx-1].Data, update.Entry.Data){
					continue
				}
				sm.log[update.Idx-1] = update.Entry
			}else{
				sm.log = append(sm.log, update.Entry)
			}
		}
		sm.lastApplied = max(sm.lastApplied, len(sm.log))
		sm.leaderId = leaderId
		actions = sm.updateCommitIndex(leaderCommit)
		action := Send{PeerId:fromId, Event:AppendEntriesRespEv{FromId:sm.id, Term:sm.currTerm, Success:true}}
		actions = append(actions, action)

		action_ := StateStore{currTerm:sm.currTerm, votedFor:sm.votedFor, log:sm.log}
		actions = append(actions, action_)

		alarm := Alarm{duration:sm.timeout + rand.Intn(sm.timeout)}
		actions = append(actions, alarm)
	}else{
		action := Send{PeerId:fromId, Event:AppendEntriesRespEv{FromId:sm.id, Term:sm.currTerm, Success:false}}
		actions = append(actions, action)
	}
	return actions
}
func (sm *StateMachine) handleAppendEntriesReq(fromId int,term int, leaderId int, prevLogIndex int, prevLogTerm int, updates []Update, leaderCommit int) []Action{
	actions := []Action{}
	switch sm.state{
		case FOLLOWER:
			actions = sm.handleAppendEntriesReqGeneric(fromId, term, leaderId, prevLogIndex, prevLogTerm, updates, leaderCommit)
		case CANDIDATE:
			if term >= sm.currTerm {
				actions = append(actions, sm.changeToFollower()...)
			}
			actions = append(actions, sm.handleAppendEntriesReqGeneric(fromId, term, leaderId, prevLogIndex, prevLogTerm, updates, leaderCommit)...)
		case LEADER:
			if term > sm.currTerm {
				actions = append(actions, sm.changeToFollower()...)
			}
			actions = append(actions, sm.handleAppendEntriesReqGeneric(fromId, term, leaderId, prevLogIndex, prevLogTerm, updates, leaderCommit)...)
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
				var _prevLogIndex, _prevLogTerm, _idx int
				if(sm.nextIndex[fromId] == 1){
					sm.lastSent[fromId] = 0

				}else{
					sm.nextIndex[fromId] -= sm.lastSent[fromId]
					_idx = min(len(sm.log), sm.nextIndex[fromId])
					_prevLogIndex = _idx-1
					debug(fmt.Sprintf("_prevLogIndex(%v) sm.id(%v) fromId(%v) nextIndex(%v) lastSend(%v) matchIndex(%v) log(%v)\n", _prevLogTerm, sm.id, fromId, sm.nextIndex,sm.lastSent, sm.matchIndex, sm.log))
					if(_prevLogIndex == 0){
						_prevLogTerm = 0
					}else{
						_prevLogTerm = sm.log[_prevLogIndex-1].Term        //'Coz log is initialized at 1 according to spec
					}
					//fmt.Printf("_prevLogIndex(%v) _prevLogTerm(%v) sm.id(%v) fromId(%v) nextIndex(%v) lastSend(%v) matchIndex(%v) log(%v)\n", _prevLogIndex, _prevLogTerm, sm.id, fromId, sm.nextIndex,sm.lastSent, sm.matchIndex, sm.log)
					update := Update{Idx:_idx, Entry:LogEntry{Term:sm.log[_idx-1].Term, Data:sm.log[_idx-1].Data}}
					updates := []Update{}
					updates = append(updates, update)
					sm.lastSent[fromId] = len(updates)
					action := Send{PeerId:fromId, Event:AppendEntriesReqEv{FromId:sm.id, Term:sm.currTerm, LeaderId: sm.id, PrevLogIndex:_prevLogIndex, PrevLogTerm:_prevLogTerm, Updates:updates, LeaderCommit:sm.commitIndex}}
					actions = append(actions, action)
				}
			}else{
				debug(fmt.Sprintf("1===================Id(%v) matchIndex(%v) nextIndex(%v) lastSent(%v)\n", sm.id, sm.matchIndex, sm.nextIndex, sm.lastSent))
				sm.matchIndex[fromId] += sm.lastSent[fromId]
				sm.nextIndex[fromId] += sm.lastSent[fromId]
				sm.lastSent[fromId] = 0
				debug(fmt.Sprintf("1===================Id(%v) matchIndex(%v) nextIndex(%v) lastSent(%v)\n", sm.id, sm.matchIndex, sm.nextIndex, sm.lastSent))
				newCommitIndex := getCommitIndex(sm.matchIndex, sm.majorityCount)
				if newCommitIndex > sm.commitIndex && sm.log[newCommitIndex-1].Term == sm.currTerm {
					oldCommitIndex := sm.commitIndex
					sm.commitIndex = newCommitIndex
					for ;oldCommitIndex < sm.commitIndex; {
						oldCommitIndex += 1
						action1 :=  Commit{index:oldCommitIndex, data:sm.log[oldCommitIndex-1].Data, err:nil}
						actions = append(actions, action1)
						action2 := LogStore{index:oldCommitIndex, data:sm.log[oldCommitIndex-1].Data}
						actions = append(actions, action2)
					}
				}
				if sm.nextIndex[fromId] <= sm.lastApplied {
					var _prevLogIndex, _prevLogTerm int
					_prevLogIndex = sm.nextIndex[fromId]-1
					if _prevLogIndex == 0 {
						_prevLogTerm = 0
					}else{
						_prevLogTerm = sm.log[_prevLogIndex-1].Term        //'Coz log is initialized at 1 according to spec
					}
					_updates := []Update{}
					idx := sm.nextIndex[fromId]
					for ;idx <= sm.lastApplied;{
						update := Update{Idx:idx,Entry:LogEntry{Term:sm.log[idx-1].Term, Data:sm.log[idx-1].Data}}	//Directly copy it
						_updates = append(_updates, update)
						idx += 1
					}
                    action := Send{fromId, AppendEntriesReqEv{FromId:sm.id, Term:sm.currTerm, LeaderId:sm.id, PrevLogIndex:_prevLogIndex, PrevLogTerm:_prevLogTerm, Updates:_updates, LeaderCommit:sm.commitIndex}}
					sm.lastSent[fromId] = len(_updates)
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
	action := StateStore{currTerm:sm.currTerm, votedFor:sm.votedFor, log:sm.log}
	actions = append(actions, action)
	action_ := Send{PeerId:candidateId, Event:VoteRespEv{FromId:sm.id, Term:sm.currTerm, VoteGranted:_voteGranted}}
	actions = append(actions, action_)
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
			entry := LogEntry{Term:sm.currTerm, Data: _data}
			sm.log = append(sm.log, entry)
			lastLogIndex := len(sm.log)
			sm.lastApplied = lastLogIndex
			for _, _peerId :=  range sm.peers{
				_updates := []Update{}
				if sm.nextIndex[_peerId] <= sm.lastApplied{
					_prevLogIndex := sm.nextIndex[_peerId]-1
					idx := sm.nextIndex[_peerId]
					for ;idx <= lastLogIndex ; {
						update := Update{Idx:idx, Entry:LogEntry{Term:sm.log[idx-1].Term, Data:sm.log[idx-1].Data}}
						_updates = append(_updates, update)
						idx += 1
					}
					_prevLogTerm := 0
					if _prevLogIndex != 0 {
						_prevLogTerm = sm.log[_prevLogIndex-1].Term
					}
                    action := Send{PeerId:_peerId, Event:AppendEntriesReqEv{FromId:sm.id, Term:sm.currTerm, LeaderId:sm.id, PrevLogIndex: _prevLogIndex, PrevLogTerm:_prevLogTerm, Updates:_updates, LeaderCommit:sm.commitIndex}}
					sm.lastSent[_peerId] = len(_updates)
					debug(fmt.Sprintf("UPDATES len : %v\n", len(_updates)))
					actions = append(actions, action)
				}
			}
			debug(fmt.Sprintf("Id(%v) lastSent(%v)\n", sm.id, sm.lastSent))
			action_ := StateStore{currTerm:sm.currTerm, votedFor:sm.votedFor, log:sm.log}
			actions = append(actions, action_)
	}
	return actions
}
//fromId : 0 for client
func (sm *StateMachine) processEvent (ev Event) []Action{
	actions := []Action{}
	switch ev.(type) {
		case AppendEntriesReqEv:
			evObj := ev.(AppendEntriesReqEv)
			actions = sm.handleAppendEntriesReq(evObj.FromId, evObj.Term, evObj.LeaderId, evObj.PrevLogIndex, evObj.PrevLogTerm, evObj.Updates, evObj.LeaderCommit)
		case AppendEntriesRespEv:
			evObj := ev.(AppendEntriesRespEv)
			actions = sm.handleAppendEntriesResp(evObj.FromId, evObj.Term, evObj.Success)
		case VoteReqEv:
			evObj := ev.(VoteReqEv)
			actions = sm.handleVoteReq(evObj.Term, evObj.CandidateId, evObj.LastLogIndex, evObj.LastLogTerm)
		case VoteRespEv:
			evObj := ev.(VoteRespEv)
			actions = sm.handleVoteResp(evObj.FromId,evObj.Term, evObj.VoteGranted)
		case TimeoutEv:
			//evObj := ev.(TimeoutEv)
			actions = sm.handleTimeout()
		case AppendEv:
			evObj := ev.(AppendEv)
			actions = sm.handleAppend(evObj.Data)
		default:
			println ("Unrecognized")
	}
	return actions
}

/*
func main(){
	rand.Seed(time.Now().UTC().UnixNano())
	sm3 := NewSm(LEADER, 3, []int{1,2})
	actions := sm3.processEvent(AppendEv{data:[]byte("first")})
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
*/
