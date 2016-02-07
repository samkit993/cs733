package main
import {
	"fmt"
	"math"
}
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

struct LogEntry{
	term int
	data []byte
}

struct EntryInfo{
	successCount int
	failCount int
	commited bool
}

struct Msg{
	from int
	to int
	command string
	action Action
}

struct StateMachine{
	id int
	currTerm int
	votedFor int
	log LogEntry[]
	commitIndex int
	lastApplied int
	nextIndex int[]
	matchIndex int[]
	currState int				//0: Follower	1:Candidate		2:Leader
	votes int					//Relevant to candidate state
	logInfo map[int]EntryInfo	//logIndex:logEntryInfo is relevant to leader state
	timeOutPeriod int			//In miliseconds
}

// IN FOLLOWER STATE
func (sm *StateMachine) Append(data []byte){
	//Do nothing (or just call current leader's append implementation)
}

func (sm *StateMachine) Timeout(){
	//If voted for no one then, become candidate
	if sm.votedFor == null {
		sm.currState = 1
		sm.currTerm += 1
		sm.votedFor = id
		sm.votes = 1
		lastLogIndex := sm.log.size()-1
		lastLogTerm := sm.log[lastLogIndex].term
		actions := []Action{}
		for peer :=  range serverlist {
			action := Send(peer.id, VoteReq(sm.currTerm, sm.id, lastLogIndex, lastLogTerm))
			append(actions, action)
		}
	}
}

func (sm *StateMachine) AppendEntriesReq(fromId int,term int, leaderId int, prevLogIndex int, prevLogTerm int, entries []byte, leaderCommit int){
	prevLogIndex -= 1 			//Log Index according to spec start at 1
	success := false 
	actions := []Action{}
	if term >= sm.currTerm && sm.log[prevLogIndex].term == prevLogTerm {
		success := true	
		sm.currTerm = term 		//Check
		entry := LogEntry{term:sm.term, entry: []byte{data}}
		if sm.log.size()-1 < prevLogIndex {
			//append the entry to log
			append(sm.log, entry)
		}else{
			//replace the entry in log
			sm.log[prevLogIndex + 1] = entry
		}
		lastApplied = max(lastApplied, prevLogIndex + 1)
		oldCommitIndex = sm.commitIndex
		if leaderCommit > sm.commitIndex {
			sm.commitIndex = min(leaderCommit, sm.log.size())
			//Accordingly LogStore action
			for ;oldCommitIndex < sm.commitIndex; {
				action := LogStore(oldCommitIndex-1, sm.log[oldCommitIndex-1])
				append(actions, action)
			}
		}
	}
	action := Send(fromId, VoteResp(sm.currTerm, success)
	append(actions, action)
}

func (sm *StateMachine) AppendEntriesResp(term int, success bool){
	//Will never get one ;-)
}

func (sm *StateMachine) VoteReq(term int, cadidateId int, lastLogIndex int,  lastLogTerm int){
	lastLogIndex -= 1 			//Log Index according to spec start at 1
	voteGranted := false
	if sm.term >= sm.currTerm {
		if sm.votedFor == nil && candidateId {  //Why "or" in spec and sir's doc
			logLen = sm.log.size()
			if sm.log[logLen-1].term > term || ( sm.log[logLen-1].term == term && sm.log.size()-1 > lastLogIndex ) {			//Check 
				sm.term = term
				sm.votedFor = candidateId
				voteGranted = true
			}
		}
	}
	action = Send(candidateId, VoteResp(sm.term, voteGranted)
}

func (sm *StateMachine) VoteResp (term int, voteGranted bool){
	//Will never get one ;-)
}

// IN CANDIDATE STATE
func (sm *StateMachine) Append(data []byte){
	//Will never get one ?
}

func (sm *StateMachine) Timeout(){
	//Restart election process
	sm.votedFor = id
	sm.votes = 1
	lastLogIndex := sm.log.size()-1
	lastLogTerm := sm.log[lastLogIndex].term
	actions := []Action{}
	for peer :=  range serverlist {
		action := Send(sm.id, VoteReq(sm.currTerm, id, lastLogIndex, lastLogTerm))
		append(actions, action)
	}
}

func (sm *StateMachine) AppendEntriesReq(term int, leaderId int, prevLogIndex int, entries []byte, leaderCommit int){
	if term > sm.currTerm {
		sm.currState = 0
		sm.votedFor = nil
		sm.votes = 0
	}else{
		//same as follower state 
	}
}
func (sm *StateMachine) AppendEntriesResp(term int, success bool){
	//Will never get one ;-)
}

func (sm *StateMachine) VoteReq(term int, cadidateId int, lastLogIndex int,  lastLogTerm){
	action = Send(candidateId, VoteResp(sm.term, false)
}

func (sm *StateMachine) VoteResp(term int, voteGranted bool){
	if voteGranted {
		votes += 1
	}
	if votes > int(ServerList.size()/2)+1 {
		//Upgraded as leader, send heartbeat to everyone
		sm.currState = 2		
		sm.votes = 0
		sm.votedFor = nil
		sm.logInfo = make(map[int]EntryInfo)		//Initializing logIndex:logCommitInfo map
		entry := LogEntry{term:sm.term, entry: []byte{}}
		prevLogIndex := log.size()-1
		prevLogTerm := sm.log[prevLogIndex-1].term
		actions := []Action{}
		for peer :=  range serverlist {
			action(peer.id, AppendEntriesReq(sm.currTerm, id, 	prevLogIndex,prevLogTerm, data, sm.commitIndex))			//Sending Heartbeat
			sm.nextIndex[peer.id] = prevLogIndex + 1
			sm.matchIndex[peer.id] = 0
			append(actions, action)
		}
	}
}


// IN LEADER STATE
func (sm *StateMachine) Append(data []byte){
	entry := LogEntry{term:sm.term, entry: []byte{data}}
	append(sm.log, entry)
	lastLogIndex := sm.log.size()
	sm.lastApplied = lastLogIndex
	actions := []Action{}
	for peer :=  range serverlist {
		if lastLogIndex >= sm.nextIndex[peer.id]{
			prevLogIndex := sm.nextIndex[peer.id]-1
			prevLogTerm := sm.log[prevLogIndex-1].term	//'Coz log is initialized at 1 according to spec
			action := send(peer.id, AppendEntriesReq(sm.currTerm, id, 	prevLogIndex,prevLogTerm, sm.log[sm.nextIndex[peer.id]].data, sm.commitIndex))
			append(actions, action)
		}
	}
}

func (sm *StateMachine) Timeout() {
	//Sending the heartbeat to everybody
	entry := LogEntry{term:sm.term, entry: []byte{}}
	prevLogIndex := log.size()-1
	prevLogTerm := sm.log[prevLogIndex-1].term
	actions := []Action{}
	for peer :=  range serverlist {
		action := Send(peer.id, AppendEntriesReq(sm.currTerm, id, 	prevLogIndex,prevLogTerm, data, sm.commitIndex))			//Sending Heartbeat
		append(actions, action)
	}

}

func (sm *StateMachine) AppendEntriesReq(term int, leaderId int, prevLogIndex int, entries []byte, leaderCommit int){
	//Should not receive one, except when there are two leaders at the time of transition(addition/deletion of new node). Yet to handle later case
	//TODO
}

func (sm *StateMachine) AppendEntriesResp(term int, success bool){
	actions := []Action{}
	if bool == false {
		sm.nextIndex[Msg.from] -= 1
		prevLogIndex := sm.nextIndex[peer.id]-1
		prevLogTerm := sm.log[prevLogIndex-1].term	//'Coz log is initialized at 1 according to spec
		action := send(peer.id, AppendEntriesReq(sm.currTerm, id, 	prevLogIndex,prevLogTerm, sm.log[sm.nextIndex[peer.id]-1].data, sm.commitIndex))
		append(actions, action)
	}else{
		sm.matchIndex[Msg.from] += 1
		idx  := sm.nextIndex[Msg.from]
		if _, ok := sm.logInfo[idx-1].successCount; !ok {
			sm.logInfo[idx-1].successCount = 1
		}else{
			sm.logInfo[idx-1].successCount += 1
		}
		if sm.logInfo[idx-1].successCount > int(ServerList.size()/2)+1 && sm.commitIndex < sm.nextIndex[Msg.from] {
			sm.logInfo[idx-1].commited = true
			sm.commitIndex += 1
			action :=  Commit(idx, sm.log[idx-1].data, nil)
			append(actions, action)
		}
		sm.nextIndex[Msg.from] += 1
		if sm.nextIndex[Msg.from] <= sm.lastApplied {
			prevLogIndex := sm.nextIndex[peer.id]-1
			prevLogTerm := sm.log[prevLogIndex-1].term	//'Coz log is initialized at 1 according to spec
			action := send(peer.id, AppendEntriesReq(sm.currTerm, id, 	prevLogIndex,prevLogTerm, sm.log[sm.nextIndex[peer.id]-1].data, sm.commitIndex))
			append(actions, action)

		}
	}
}
func (stateMachine *StateMachine) VoteReq(term int, cadidateId int, lastLogIndex int,  lastLogTerm) {}	//Will never get called ;-)
func (stateMachine *StateMachine) VoteResp(term int, voteGranted bool) {} //Will never get called ;-)
