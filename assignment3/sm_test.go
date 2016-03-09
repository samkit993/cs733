package main
import (
	"fmt"
	"testing"
)

func checkError(t *testing.T, err error){
	if err != nil {
		t.Error(fmt.Sprintf("Error occurred: %s", err.Error())) // t.Error is visible when running `go test -verbose`
	}
}

func expect(t *testing.T, a string, b string) {
	if a != b {
		t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
	}
}
/*
func testElectionTimeout(sm *StateMachine) {
	var ev = Timeout{}
	actions = sm.processEvent(ev)
	expect sm.state == candidate
	expect Send(VoteRequest to other peers) in actions.
}
*/
type SmMap map[int]*StateMachine
type SystemLog []string
func (smMap SmMap) printId(idx int){
	fmt.Println((*(smMap[idx])).String())
}
func (smMap *SmMap) printAll(){
	for _, smPtr := range *smMap{
		fmt.Println((*(smPtr)).String())
	}
}
func (actionLog SystemLog) getAll() string{
	s := ""
	for _, sysLogEntry := range actionLog{
		s += sysLogEntry
	}
	return s
}
func (actionLog SystemLog) print(){
	for _, sysLogEntry := range actionLog{
		fmt.Println(sysLogEntry)
	}
}
func (actionLog *SystemLog) add(entry string){
	*actionLog = append(*actionLog, entry)
}

func formMsgs(actions []Action, _originId int) []Msg{
	msgs := []Msg{}
	for _,_action := range actions{
		msgs = append(msgs, Msg{originId:_originId, action:_action})
	}
	return msgs
}
/*
 * Plays all the send actions on respective state machines
 * Continue this until there are no more actions
 * Meanwhile, store all the RPC calls and actions generated in msgLog and actionLog
 */
func processSendAndLogRest(msgs []Msg, smMap SmMap, actionLog *SystemLog, msgLog *SystemLog, processSend bool){
	resp_msgs := []Msg{}
	for {
		for _, msg := range msgs {
			msgLog.add(fmt.Sprintf("%v", msg))
			switch msg.action.(type){
			case Send:
				if !processSend {
					continue
				}
				actObj := msg.action.(Send)
				smPtr, found := smMap[actObj.peerId]
				if found{
					_resp_actions := smPtr.processEvent(msg.originId, actObj.event)
					_resp_msgs := formMsgs(_resp_actions, actObj.peerId)
					resp_msgs = append(resp_msgs, _resp_msgs...)
				}
			case Commit:
				actObj := msg.action.(Commit)
				(*actionLog).add(fmt.Sprintf("smId(%v) Action(%v)", msg.originId,actObj.String()))
			case LogStore:
				actObj := msg.action.(LogStore)
				(*actionLog).add(fmt.Sprintf("smId(%v) Action(%v)", msg.originId,actObj.String()))
			case Alarm:
				actObj := msg.action.(Alarm)
				(*actionLog).add(fmt.Sprintf("smId(%v) Action(%v)", msg.originId,actObj.String()))
			}
		}
		if len(resp_msgs) == 0 {
			break
		}else {
			msgs = []Msg{}
			msgs = append(msgs, resp_msgs...)
			resp_msgs = []Msg{}
		}
	}
}
/*
 * Below function tries to simulate real behavior
 * It mainly tests three events/RPC calls: Append, AppendEntriesReq, AppendEntriesResp
 */
func TestAppend(t *testing.T){
	sm1 := NewSm(FOLLOWER, 1, []int{2,3})
	sm2 := NewSm(FOLLOWER, 2, []int{1,3})
	sm3 := NewSm(LEADER, 3, []int{1,2})
	smMap := make(SmMap)
	smMap[1] = &sm1
	smMap[2] = &sm2
	smMap[3] = &sm3

	actionLog := SystemLog{}
	msgLog := SystemLog{}

	actions := smMap[3].processEvent(0, AppendEv{data:[]byte("first")})
	msgs := formMsgs(actions, 3)
	processSendAndLogRest(msgs, smMap, &actionLog, &msgLog, true)
	actions = sm3.processEvent(0, AppendEv{data:[]byte("second")})
	msgs = formMsgs(actions, 3)
	processSendAndLogRest(msgs, smMap, &actionLog, &msgLog, true)
	actions = sm3.processEvent(0, AppendEv{data:[]byte("third")})
	msgs = formMsgs(actions, 3)
	processSendAndLogRest(msgs, smMap, &actionLog, &msgLog, true)

	//actionLog.print()
	//msgLog.print()
	expectedActionLog := SystemLog{}
	expectedActionLog.add("smId(3) Action(Commit:idx(1) data(first) error(<nil>))")
	expectedActionLog.add("smId(3) Action(LogStore:idx(1) data(first))")
	expectedActionLog.add("smId(1) Action(Commit:idx(1) data(first) error(<nil>))")
	expectedActionLog.add("smId(1) Action(LogStore:idx(1) data(first))")
	expectedActionLog.add("smId(2) Action(Commit:idx(1) data(first) error(<nil>))")
	expectedActionLog.add("smId(2) Action(LogStore:idx(1) data(first))")
	expectedActionLog.add("smId(3) Action(Commit:idx(2) data(second) error(<nil>))")
	expectedActionLog.add("smId(3) Action(LogStore:idx(2) data(second))")
	expectedActionLog.add("smId(1) Action(Commit:idx(2) data(second) error(<nil>))")
	expectedActionLog.add("smId(1) Action(LogStore:idx(2) data(second))")
	expectedActionLog.add("smId(2) Action(Commit:idx(2) data(second) error(<nil>))")
	expectedActionLog.add("smId(2) Action(LogStore:idx(2) data(second))")
	expectedActionLog.add("smId(3) Action(Commit:idx(3) data(third) error(<nil>))")
	expectedActionLog.add("smId(3) Action(LogStore:idx(3) data(third))")

	expect(t, actionLog.getAll(), expectedActionLog.getAll())

	expectedMsgLog := SystemLog{}
	expectedMsgLog.add("Msg: originId(3) action(Send:peerId(1) event(AppendEntriesReqEv:term(0) leaderId(3) prevLogIndex(0) prevLogTerm(0) entries([0:first]) leaderCommit(0)))")
	expectedMsgLog.add("Msg: originId(3) action(Send:peerId(2) event(AppendEntriesReqEv:term(0) leaderId(3) prevLogIndex(0) prevLogTerm(0) entries([0:first]) leaderCommit(0)))")
	expectedMsgLog.add("Msg: originId(1) action(Send:peerId(3) event(AppendEntriesRespEv:term(0) success(true)))")
	expectedMsgLog.add("Msg: originId(2) action(Send:peerId(3) event(AppendEntriesRespEv:term(0) success(true)))")
	expectedMsgLog.add("Msg: originId(3) action(Commit:idx(1) data(first) error(<nil>))")
	expectedMsgLog.add("Msg: originId(3) action(LogStore:idx(1) data(first))")
	expectedMsgLog.add("Msg: originId(3) action(Send:peerId(1) event(AppendEntriesReqEv:term(0) leaderId(3) prevLogIndex(1) prevLogTerm(0) entries([0:second]) leaderCommit(1)))")
	expectedMsgLog.add("Msg: originId(3) action(Send:peerId(2) event(AppendEntriesReqEv:term(0) leaderId(3) prevLogIndex(1) prevLogTerm(0) entries([0:second]) leaderCommit(1)))")
	expectedMsgLog.add("Msg: originId(1) action(Commit:idx(1) data(first) error(<nil>))")
	expectedMsgLog.add("Msg: originId(1) action(LogStore:idx(1) data(first))")
	expectedMsgLog.add("Msg: originId(1) action(Send:peerId(3) event(AppendEntriesRespEv:term(0) success(true)))")
	expectedMsgLog.add("Msg: originId(2) action(Commit:idx(1) data(first) error(<nil>))")
	expectedMsgLog.add("Msg: originId(2) action(LogStore:idx(1) data(first))")
	expectedMsgLog.add("Msg: originId(2) action(Send:peerId(3) event(AppendEntriesRespEv:term(0) success(true)))")
	expectedMsgLog.add("Msg: originId(3) action(Commit:idx(2) data(second) error(<nil>))")
	expectedMsgLog.add("Msg: originId(3) action(LogStore:idx(2) data(second))")
	expectedMsgLog.add("Msg: originId(3) action(Send:peerId(1) event(AppendEntriesReqEv:term(0) leaderId(3) prevLogIndex(2) prevLogTerm(0) entries([0:third]) leaderCommit(2)))")
	expectedMsgLog.add("Msg: originId(3) action(Send:peerId(2) event(AppendEntriesReqEv:term(0) leaderId(3) prevLogIndex(2) prevLogTerm(0) entries([0:third]) leaderCommit(2)))")
	expectedMsgLog.add("Msg: originId(1) action(Commit:idx(2) data(second) error(<nil>))")
	expectedMsgLog.add("Msg: originId(1) action(LogStore:idx(2) data(second))")
	expectedMsgLog.add("Msg: originId(1) action(Send:peerId(3) event(AppendEntriesRespEv:term(0) success(true)))")
	expectedMsgLog.add("Msg: originId(2) action(Commit:idx(2) data(second) error(<nil>))")
	expectedMsgLog.add("Msg: originId(2) action(LogStore:idx(2) data(second))")
	expectedMsgLog.add("Msg: originId(2) action(Send:peerId(3) event(AppendEntriesRespEv:term(0) success(true)))")
	expectedMsgLog.add("Msg: originId(3) action(Commit:idx(3) data(third) error(<nil>))")
	expectedMsgLog.add("Msg: originId(3) action(LogStore:idx(3) data(third))")

	expect(t, msgLog.getAll(), expectedMsgLog.getAll())
}

func TestLeaderTimeout(t *testing.T) {
	smMap := make(SmMap)
	smMap[1] = &StateMachine{id:1, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:2,lastApplied:2, nextIndex:make(map[int]int), matchIndex:make(map[int]int),lastSent:make(map[int]int), state:FOLLOWER, votes:make(map[int]bool),peers:[]int{2,3}, majorityCount:2}
	(*(smMap[1])).log = append((*(smMap[1])).log, LogEntry{term:0, data:[]byte("first")})
	(*(smMap[1])).log = append((*(smMap[1])).log, LogEntry{term:0, data:[]byte("second")})
	(*(smMap[1])).log = append((*(smMap[1])).log, LogEntry{term:0, data:[]byte("third")})

	smMap[2] = &StateMachine{id:2, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:2,lastApplied:2, nextIndex:make(map[int]int), matchIndex:make(map[int]int),lastSent:make(map[int]int), state:FOLLOWER, votes:make(map[int]bool),peers:[]int{1,3}, majorityCount:2}
	(*(smMap[2])).log = append((*(smMap[2])).log, LogEntry{term:0, data:[]byte("first")})
	(*(smMap[2])).log = append((*(smMap[2])).log, LogEntry{term:0, data:[]byte("second")})
	(*(smMap[2])).log = append((*(smMap[2])).log, LogEntry{term:0, data:[]byte("third")})

	smMap[3] = &StateMachine{id:3, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:2,lastApplied:3, nextIndex:make(map[int]int), matchIndex:make(map[int]int),lastSent:make(map[int]int), state:LEADER, votes:make(map[int]bool),peers:[]int{1,2}, majorityCount:2}
	(*(smMap[3])).log = append((*(smMap[3])).log, LogEntry{term:0, data:[]byte("first")})
	(*(smMap[3])).log = append((*(smMap[3])).log, LogEntry{term:0, data:[]byte("second")})
	(*(smMap[3])).log = append((*(smMap[3])).log, LogEntry{term:0, data:[]byte("third")})

	(*(smMap[3])).nextIndex[1] = 4
	(*(smMap[3])).nextIndex[2] = 4
	(*(smMap[3])).matchIndex[1] = 3
	(*(smMap[3])).matchIndex[2] = 3
	(*(smMap[3])).lastSent[1] = 0
	(*(smMap[3])).lastSent[2] = 0

	//fmt.Println(smMap[1])
	//fmt.Println(smMap[2])
	//fmt.Println(smMap[3])

	actions := smMap[3].processEvent(0, TimeoutEv{})

	expectedActions := []Action{}
	log := []LogEntry{}
	log = append(log, LogEntry{term:0, data:[]byte{}})
	expectedActions = append(expectedActions, Send{peerId:1, event:AppendEntriesReqEv{term:0, leaderId:3, prevLogIndex:2, prevLogTerm:0, entries:log, leaderCommit:2}})
	expectedActions = append(expectedActions, Send{peerId:2, event:AppendEntriesReqEv{term:0, leaderId:3, prevLogIndex:2, prevLogTerm:0, entries:log, leaderCommit:2}})
	expectedActions = append(expectedActions, Alarm{duration:heartBeatTimeout})

	expect(t, fmt.Sprintf("%v", actions), fmt.Sprintf("%v", expectedActions))
}

func TestFollowerTimeout(t *testing.T) {
	smMap := make(SmMap)
	smMap[1] = &StateMachine{id:1, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:2,lastApplied:2, nextIndex:make(map[int]int), matchIndex:make(map[int]int),lastSent:make(map[int]int), state:FOLLOWER, votes:make(map[int]bool),peers:[]int{2,3}, majorityCount:2}
	(*(smMap[1])).log = append((*(smMap[1])).log, LogEntry{term:0, data:[]byte("first")})
	(*(smMap[1])).log = append((*(smMap[1])).log, LogEntry{term:0, data:[]byte("second")})
	(*(smMap[1])).log = append((*(smMap[1])).log, LogEntry{term:0, data:[]byte("third")})

	smMap[2] = &StateMachine{id:2, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:2,lastApplied:2, nextIndex:make(map[int]int), matchIndex:make(map[int]int),lastSent:make(map[int]int), state:FOLLOWER, votes:make(map[int]bool),peers:[]int{1,3}, majorityCount:2}
	(*(smMap[2])).log = append((*(smMap[2])).log, LogEntry{term:0, data:[]byte("first")})
	(*(smMap[2])).log = append((*(smMap[2])).log, LogEntry{term:0, data:[]byte("second")})
	(*(smMap[2])).log = append((*(smMap[2])).log, LogEntry{term:0, data:[]byte("third")})

	smMap[3] = &StateMachine{id:3, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:2,lastApplied:3, nextIndex:make(map[int]int), matchIndex:make(map[int]int),lastSent:make(map[int]int), state:LEADER, votes:make(map[int]bool),peers:[]int{1,2}, majorityCount:2}
	(*(smMap[3])).log = append((*(smMap[3])).log, LogEntry{term:0, data:[]byte("first")})
	(*(smMap[3])).log = append((*(smMap[3])).log, LogEntry{term:0, data:[]byte("second")})
	(*(smMap[3])).log = append((*(smMap[3])).log, LogEntry{term:0, data:[]byte("third")})

	(*(smMap[3])).nextIndex[1] = 4
	(*(smMap[3])).nextIndex[2] = 4
	(*(smMap[3])).matchIndex[1] = 3
	(*(smMap[3])).matchIndex[2] = 3
	(*(smMap[3])).lastSent[1] = 0
	(*(smMap[3])).lastSent[2] = 0

	//fmt.Println(smMap[1])
	//fmt.Println(smMap[2])
	//fmt.Println(smMap[3])

	actionLog := SystemLog{}
	msgLog := SystemLog{}

	actions := smMap[1].processEvent(0, TimeoutEv{})
	msgs := formMsgs(actions, 1)
	processSendAndLogRest(msgs, smMap, &actionLog, &msgLog, true)

	expectedMsgLog := SystemLog{}
	expectedMsgLog.add("Msg: originId(1) action(Send:peerId(2) event(VoteReqEv:term(1) cadidateId(1) lastLogIndex(2) lastLogTerm(0)))")
	expectedMsgLog.add("Msg: originId(1) action(Send:peerId(3) event(VoteReqEv:term(1) cadidateId(1) lastLogIndex(2) lastLogTerm(0)))")
	expectedMsgLog.add("Msg: originId(1) action(Alarm:duration(21))")
	expectedMsgLog.add("Msg: originId(2) action(Send:peerId(1) event(VoteRespEv:term(0) voteGranted(true)))")
	expectedMsgLog.add("Msg: originId(3) action(Send:peerId(1) event(VoteRespEv:term(0) voteGranted(false)))")
	expectedMsgLog.add("Msg: originId(1) action(Send:peerId(2) event(AppendEntriesReqEv:term(1) leaderId(1) prevLogIndex(2) prevLogTerm(0) entries([1:]) leaderCommit(2)))")
	expectedMsgLog.add("Msg: originId(1) action(Send:peerId(3) event(AppendEntriesReqEv:term(1) leaderId(1) prevLogIndex(2) prevLogTerm(0) entries([1:]) leaderCommit(2)))")
	expectedMsgLog.add("Msg: originId(1) action(Alarm:duration(10))")
	expectedMsgLog.add("Msg: originId(2) action(Send:peerId(1) event(AppendEntriesRespEv:term(1) success(true)))")
	expectedMsgLog.add("Msg: originId(3) action(Send:peerId(1) event(AppendEntriesRespEv:term(1) success(true)))")
	expect(t, fmt.Sprintf("%v", msgLog.getAll()), fmt.Sprintf("%v", expectedMsgLog.getAll()))

}

func TestCandidateTimeout(t *testing.T) {
	smMap := make(SmMap)
	smMap[1] = &StateMachine{id:1, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:2, lastApplied:2, nextIndex:make(map[int]int), matchIndex:make(map[int]int), lastSent:make(map[int]int), state:FOLLOWER, votes:make(map[int]bool), peers:[]int{2, 3}, majorityCount:2}
	(*(smMap[1])).log = append((*(smMap[1])).log, LogEntry{term:0, data:[]byte("first")})
	(*(smMap[1])).log = append((*(smMap[1])).log, LogEntry{term:0, data:[]byte("second")})
	(*(smMap[1])).log = append((*(smMap[1])).log, LogEntry{term:0, data:[]byte("third")})

	smMap[2] = &StateMachine{id:2, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:2, lastApplied:2, nextIndex:make(map[int]int), matchIndex:make(map[int]int), lastSent:make(map[int]int), state:FOLLOWER, votes:make(map[int]bool), peers:[]int{1, 3}, majorityCount:2}
	(*(smMap[2])).log = append((*(smMap[2])).log, LogEntry{term:0, data:[]byte("first")})
	(*(smMap[2])).log = append((*(smMap[2])).log, LogEntry{term:0, data:[]byte("second")})
	(*(smMap[2])).log = append((*(smMap[2])).log, LogEntry{term:0, data:[]byte("third")})

	smMap[3] = &StateMachine{id:3, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:2, lastApplied:3, nextIndex:make(map[int]int), matchIndex:make(map[int]int), lastSent:make(map[int]int), state:LEADER, votes:make(map[int]bool), peers:[]int{1, 2}, majorityCount:2}
	(*(smMap[3])).log = append((*(smMap[3])).log, LogEntry{term:0, data:[]byte("first")})
	(*(smMap[3])).log = append((*(smMap[3])).log, LogEntry{term:0, data:[]byte("second")})
	(*(smMap[3])).log = append((*(smMap[3])).log, LogEntry{term:0, data:[]byte("third")})

	(*(smMap[3])).nextIndex[1] = 4
	(*(smMap[3])).nextIndex[2] = 4
	(*(smMap[3])).matchIndex[1] = 3
	(*(smMap[3])).matchIndex[2] = 3
	(*(smMap[3])).lastSent[1] = 0
	(*(smMap[3])).lastSent[2] = 0

	actionLog := SystemLog{}
	msgLog := SystemLog{}

	actions := smMap[1].processEvent(0, TimeoutEv{})
	msgs := formMsgs(actions, 1)
	processSendAndLogRest(msgs, smMap, &actionLog, &msgLog, false)		//Candidate will not hear back from peers
	expect(t, "1 0 2", fmt.Sprintf("%v %v %v", smMap[1].state, smMap[2].state, smMap[3].state))	//Candidate Follower Leader

	actions = smMap[2].processEvent(0, TimeoutEv{})
	msgs = formMsgs(actions, 2)
	processSendAndLogRest(msgs, smMap, &actionLog, &msgLog, false)		//Candidate will not hear back from peers
	expect(t, "1 1 2", fmt.Sprintf("%v %v %v", smMap[1].state, smMap[2].state, smMap[3].state))	//Candidate Candidate Leader

	actions = smMap[2].processEvent(0, TimeoutEv{})
	msgs = formMsgs(actions, 2)
	processSendAndLogRest(msgs, smMap, &actionLog, &msgLog, true)
	expect(t, "0 2 0", fmt.Sprintf("%v %v %v", smMap[1].state, smMap[2].state, smMap[3].state))	//Follower Leader Follower
	expect(t, "2 2 2", fmt.Sprintf("%v %v %v", smMap[1].currTerm, smMap[2].currTerm, smMap[3].currTerm))

	//actionLog.print()
	//msgLog.print()
}
