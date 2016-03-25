package main
import (
	"fmt"
	"testing"
)

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
		s += sysLogEntry + "\n"
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
		msgs = append(msgs, Msg{OriginId:_originId, Action:_action})
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
			switch msg.Action.(type){
			case Send:
				if !processSend {
					continue
				}
				actObj := msg.Action.(Send)
				smPtr, found := smMap[actObj.PeerId]
				if found{
					_resp_actions := smPtr.processEvent(actObj.Event)
					_resp_msgs := formMsgs(_resp_actions, actObj.PeerId)
					resp_msgs = append(resp_msgs, _resp_msgs...)
				}
				msgLog.add(fmt.Sprintf("%v", msg))
			case Commit:
				actObj := msg.Action.(Commit)
				(*actionLog).add(fmt.Sprintf("smId(%v) Action(%v)", msg.OriginId,actObj.String()))
				msgLog.add(fmt.Sprintf("%v", msg))
			case LogStore:
				actObj := msg.Action.(LogStore)
				(*actionLog).add(fmt.Sprintf("smId(%v) Action(%v)", msg.OriginId,actObj.String()))
				msgLog.add(fmt.Sprintf("%v", msg))
			case StateStore:
				actObj := msg.Action.(StateStore)
				(*actionLog).add(fmt.Sprintf("smId(%v) Action(%v)", msg.OriginId,actObj.String()))
				msgLog.add(fmt.Sprintf("%v", msg))
			case Alarm:
				_ = msg.Action.(Alarm)
				//(*actionLog).add(fmt.Sprintf("smId(%v) Action(%v)", msg.OriginId,actObj.String()))
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
	sm1,_ := NewSm(FOLLOWER, 1, []int{2,3}, 150, 250)
	sm2,_ := NewSm(FOLLOWER, 2, []int{1,3}, 150, 250)
	sm3,alarm3 := NewSm(LEADER, 3, []int{1,2}, 150, 250)
	expect(t, fmt.Sprintf("%v", alarm3.duration), "150")
	smMap := make(SmMap)
	smMap[1] = &sm1
	smMap[2] = &sm2
	smMap[3] = &sm3

	actionLog := SystemLog{}
	msgLog := SystemLog{}

	actions := smMap[3].processEvent(AppendEv{Data:[]byte("first")})
	msgs := formMsgs(actions, 3)
	processSendAndLogRest(msgs, smMap, &actionLog, &msgLog, true)
	actions = sm3.processEvent(AppendEv{Data:[]byte("second")})
	msgs = formMsgs(actions, 3)
	processSendAndLogRest(msgs, smMap, &actionLog, &msgLog, true)
	actions = sm3.processEvent(AppendEv{Data:[]byte("third")})
	msgs = formMsgs(actions, 3)
	processSendAndLogRest(msgs, smMap, &actionLog, &msgLog, true)

	//actionLog.print()
	//msgLog.print()

	//actionLog.print()
	expect(t, fmt.Sprintf("%d", sm1.commitIndex), "2")
	expect(t, fmt.Sprintf("%d", sm2.commitIndex), "2")
	expect(t, fmt.Sprintf("%d", sm3.commitIndex), "3")

	//msgLog.print()
}

func TestLeaderTimeout(t *testing.T) {
	smMap := make(SmMap)
	smMap[1] = &StateMachine{id:1, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:2,lastApplied:2, nextIndex:make(map[int]int), matchIndex:make(map[int]int),lastSent:make(map[int]int), state:FOLLOWER, votes:make(map[int]bool),peers:[]int{2,3}, majorityCount:2, heartBeatTimeout:150, timeout:250}
	(*(smMap[1])).log = append((*(smMap[1])).log, LogEntry{Term:0, Data:[]byte("first")})
	(*(smMap[1])).log = append((*(smMap[1])).log, LogEntry{Term:0, Data:[]byte("second")})
	(*(smMap[1])).log = append((*(smMap[1])).log, LogEntry{Term:0, Data:[]byte("third")})

	smMap[2] = &StateMachine{id:2, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:2,lastApplied:2, nextIndex:make(map[int]int), matchIndex:make(map[int]int),lastSent:make(map[int]int), state:FOLLOWER, votes:make(map[int]bool),peers:[]int{1,3}, majorityCount:2, heartBeatTimeout:150, timeout:250}
	(*(smMap[2])).log = append((*(smMap[2])).log, LogEntry{Term:0, Data:[]byte("first")})
	(*(smMap[2])).log = append((*(smMap[2])).log, LogEntry{Term:0, Data:[]byte("second")})
	(*(smMap[2])).log = append((*(smMap[2])).log, LogEntry{Term:0, Data:[]byte("third")})

	smMap[3] = &StateMachine{id:3, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:2,lastApplied:3, nextIndex:make(map[int]int), matchIndex:make(map[int]int),lastSent:make(map[int]int), state:LEADER, votes:make(map[int]bool),peers:[]int{1,2}, majorityCount:2,heartBeatTimeout:150, timeout:250}
	(*(smMap[3])).log = append((*(smMap[3])).log, LogEntry{Term:0, Data:[]byte("first")})
	(*(smMap[3])).log = append((*(smMap[3])).log, LogEntry{Term:0, Data:[]byte("second")})
	(*(smMap[3])).log = append((*(smMap[3])).log, LogEntry{Term:0, Data:[]byte("third")})

	(*(smMap[3])).nextIndex[1] = 4
	(*(smMap[3])).nextIndex[2] = 4
	(*(smMap[3])).matchIndex[1] = 3
	(*(smMap[3])).matchIndex[2] = 3
	(*(smMap[3])).lastSent[1] = 0
	(*(smMap[3])).lastSent[2] = 0

	//fmt.Println(smMap[1])
	//fmt.Println(smMap[2])
	//fmt.Println(smMap[3])

	actions := smMap[3].processEvent(TimeoutEv{})

	expectedActions := []Action{}
	update := Update{Idx:-1, Entry:LogEntry{Term:0, Data:[]byte{}}}
	updates := []Update{}
	updates = append(updates, update)
	expectedActions = append(expectedActions, Send{PeerId:1, Event:AppendEntriesReqEv{FromId:3, Term:0, LeaderId:3, PrevLogIndex:3, PrevLogTerm:0, Updates:updates, LeaderCommit:2}})
	expectedActions = append(expectedActions, Send{PeerId:2, Event:AppendEntriesReqEv{FromId:3, Term:0, LeaderId:3, PrevLogIndex:3, PrevLogTerm:0, Updates:updates, LeaderCommit:2}})
	expectedActions = append(expectedActions, Alarm{duration:150})

	expect(t, fmt.Sprintf("%v", expectedActions), fmt.Sprintf("%v", actions))
}

func TestFollowerTimeout(t *testing.T) {
	smMap := make(SmMap)
	smMap[1] = &StateMachine{id:1, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:2,lastApplied:2, nextIndex:make(map[int]int), matchIndex:make(map[int]int),lastSent:make(map[int]int), state:FOLLOWER, votes:make(map[int]bool),peers:[]int{2,3}, majorityCount:2, heartBeatTimeout:150, timeout:250}
	(*(smMap[1])).log = append((*(smMap[1])).log, LogEntry{Term:0, Data:[]byte("first")})
	(*(smMap[1])).log = append((*(smMap[1])).log, LogEntry{Term:0, Data:[]byte("second")})
	(*(smMap[1])).log = append((*(smMap[1])).log, LogEntry{Term:0, Data:[]byte("third")})

	smMap[2] = &StateMachine{id:2, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:2,lastApplied:2, nextIndex:make(map[int]int), matchIndex:make(map[int]int),lastSent:make(map[int]int), state:FOLLOWER, votes:make(map[int]bool),peers:[]int{1,3}, majorityCount:2, heartBeatTimeout:150, timeout:250}
	(*(smMap[2])).log = append((*(smMap[2])).log, LogEntry{Term:0, Data:[]byte("first")})
	(*(smMap[2])).log = append((*(smMap[2])).log, LogEntry{Term:0, Data:[]byte("second")})
	(*(smMap[2])).log = append((*(smMap[2])).log, LogEntry{Term:0, Data:[]byte("third")})

	smMap[3] = &StateMachine{id:3, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:2,lastApplied:3, nextIndex:make(map[int]int), matchIndex:make(map[int]int),lastSent:make(map[int]int), state:LEADER, votes:make(map[int]bool),peers:[]int{1,2}, majorityCount:2, heartBeatTimeout:150, timeout:250}
	(*(smMap[3])).log = append((*(smMap[3])).log, LogEntry{Term:0, Data:[]byte("first")})
	(*(smMap[3])).log = append((*(smMap[3])).log, LogEntry{Term:0, Data:[]byte("second")})
	(*(smMap[3])).log = append((*(smMap[3])).log, LogEntry{Term:0, Data:[]byte("third")})

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

	//Actual Test Begins
	actions := smMap[1].processEvent(TimeoutEv{})
	msgs := formMsgs(actions, 1)
	processSendAndLogRest(msgs, smMap, &actionLog, &msgLog, true)

	expectedMsgLog := SystemLog{}
	expectedMsgLog.add("Msg: originId(1) action(Send:peerId(2) event(VoteReqEv:term(1) cadidateId(1) lastLogIndex(3) lastLogTerm(0)))")
	expectedMsgLog.add("Msg: originId(1) action(Send:peerId(3) event(VoteReqEv:term(1) cadidateId(1) lastLogIndex(3) lastLogTerm(0)))")
	expectedMsgLog.add("Msg: originId(1) action(StateStore:currTerm(1) votedFor(1) log([0:first0:second0:third]))")
	expectedMsgLog.add("Msg: originId(2) action(StateStore:currTerm(0) votedFor(1) log([0:first0:second0:third]))")
	expectedMsgLog.add("Msg: originId(2) action(Send:peerId(1) event(VoteRespEv:fromId(2) term(0) voteGranted(true)))")
	expectedMsgLog.add("Msg: originId(3) action(StateStore:currTerm(0) votedFor(-1) log([0:first0:second0:third]))")
	expectedMsgLog.add("Msg: originId(3) action(Send:peerId(1) event(VoteRespEv:fromId(3) term(0) voteGranted(false)))")
	expectedMsgLog.add("Msg: originId(1) action(Send:peerId(2) event(AppendEntriesReqEv:fromId(1) term(1) leaderId(1) prevLogIndex(3) prevLogTerm(0) entries(Idx(-1)LogEntry(1:)) leaderCommit(2)))")
	expectedMsgLog.add("Msg: originId(1) action(Send:peerId(3) event(AppendEntriesReqEv:fromId(1) term(1) leaderId(1) prevLogIndex(3) prevLogTerm(0) entries(Idx(-1)LogEntry(1:)) leaderCommit(2)))")
	expectedMsgLog.add("Msg: originId(1) action(StateStore:currTerm(1) votedFor(-1) log([0:first0:second0:third]))")
	expectedMsgLog.add("Msg: originId(2) action(Send:peerId(1) event(AppendEntriesRespEv:fromId(2) term(1) success(true)))")
	expectedMsgLog.add("Msg: originId(2) action(StateStore:currTerm(1) votedFor(1) log([0:first0:second0:third]))")
	expectedMsgLog.add("Msg: originId(3) action(Send:peerId(1) event(AppendEntriesRespEv:fromId(3) term(1) success(true)))")
	expectedMsgLog.add("Msg: originId(3) action(StateStore:currTerm(1) votedFor(-1) log([0:first0:second0:third]))")
	expect(t, fmt.Sprintf("%v", expectedMsgLog.getAll()), fmt.Sprintf("%v", msgLog.getAll()))
	//msgLog.print()
}

func TestCandidateTimeout(t *testing.T) {
	smMap := make(SmMap)
	smMap[1] = &StateMachine{id:1, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:2, lastApplied:2, nextIndex:make(map[int]int), matchIndex:make(map[int]int), lastSent:make(map[int]int), state:FOLLOWER, votes:make(map[int]bool), peers:[]int{2, 3}, majorityCount:2, heartBeatTimeout:150, timeout:250}
	(*(smMap[1])).log = append((*(smMap[1])).log, LogEntry{Term:0, Data:[]byte("first")})
	(*(smMap[1])).log = append((*(smMap[1])).log, LogEntry{Term:0, Data:[]byte("second")})
	(*(smMap[1])).log = append((*(smMap[1])).log, LogEntry{Term:0, Data:[]byte("third")})

	smMap[2] = &StateMachine{id:2, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:2, lastApplied:2, nextIndex:make(map[int]int), matchIndex:make(map[int]int), lastSent:make(map[int]int), state:FOLLOWER, votes:make(map[int]bool), peers:[]int{1, 3}, majorityCount:2, heartBeatTimeout:150, timeout:250 }
	(*(smMap[2])).log = append((*(smMap[2])).log, LogEntry{Term:0, Data:[]byte("first")})
	(*(smMap[2])).log = append((*(smMap[2])).log, LogEntry{Term:0, Data:[]byte("second")})
	(*(smMap[2])).log = append((*(smMap[2])).log, LogEntry{Term:0, Data:[]byte("third")})

	smMap[3] = &StateMachine{id:3, currTerm:0, votedFor:-1, log:[]LogEntry{}, commitIndex:2, lastApplied:3, nextIndex:make(map[int]int), matchIndex:make(map[int]int), lastSent:make(map[int]int), state:LEADER, votes:make(map[int]bool), peers:[]int{1, 2}, majorityCount:2, heartBeatTimeout:150, timeout:250 }
	(*(smMap[3])).log = append((*(smMap[3])).log, LogEntry{Term:0, Data:[]byte("first")})
	(*(smMap[3])).log = append((*(smMap[3])).log, LogEntry{Term:0, Data:[]byte("second")})
	(*(smMap[3])).log = append((*(smMap[3])).log, LogEntry{Term:0, Data:[]byte("third")})

	(*(smMap[3])).nextIndex[1] = 4
	(*(smMap[3])).nextIndex[2] = 4
	(*(smMap[3])).matchIndex[1] = 3
	(*(smMap[3])).matchIndex[2] = 3
	(*(smMap[3])).lastSent[1] = 0
	(*(smMap[3])).lastSent[2] = 0

	actionLog := SystemLog{}
	msgLog := SystemLog{}

	//Actual Test Begins
	actions := smMap[1].processEvent(TimeoutEv{})
	msgs := formMsgs(actions, 1)
	processSendAndLogRest(msgs, smMap, &actionLog, &msgLog, false)		//Candidate will not hear back from peers
	expect(t, "1 0 2", fmt.Sprintf("%v %v %v", smMap[1].state, smMap[2].state, smMap[3].state))	//Candidate Follower Leader

	actions = smMap[2].processEvent(TimeoutEv{})
	msgs = formMsgs(actions, 2)
	processSendAndLogRest(msgs, smMap, &actionLog, &msgLog, false)		//Candidate will not hear back from peers
	expect(t, "1 1 2", fmt.Sprintf("%v %v %v", smMap[1].state, smMap[2].state, smMap[3].state))	//Candidate Candidate Leader

	actions = smMap[2].processEvent(TimeoutEv{})
	msgs = formMsgs(actions, 2)
	processSendAndLogRest(msgs, smMap, &actionLog, &msgLog, true)
	expect(t, "0 2 0", fmt.Sprintf("%v %v %v", smMap[1].state, smMap[2].state, smMap[3].state))	//Follower Leader Follower
	expect(t, "2 2 2", fmt.Sprintf("%v %v %v", smMap[1].currTerm, smMap[2].currTerm, smMap[3].currTerm))

	//actionLog.print()
	//msgLog.print()
}
