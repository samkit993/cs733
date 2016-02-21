package assignment2
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
func playSendAndLogRest(msgs []Msg, smMap SmMap, actionLog *SystemLog, msgLog *SystemLog){
	resp_msgs := []Msg{}
	for {
		for _, msg := range msgs {
			msgLog.add(fmt.Sprintf("%v", msg))
			switch msg.action.(type){
			case Send:
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
	playSendAndLogRest(msgs, smMap, &actionLog, &msgLog)
	actions = sm3.processEvent(0, AppendEv{data:[]byte("second")})
	msgs = formMsgs(actions, 3)
	playSendAndLogRest(msgs, smMap, &actionLog, &msgLog)
	actions = sm3.processEvent(0, AppendEv{data:[]byte("third")})
	msgs = formMsgs(actions, 3)
	playSendAndLogRest(msgs, smMap, &actionLog, &msgLog)

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
