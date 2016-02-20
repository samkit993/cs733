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
	actions = sm.ProcessEvent(ev)
	expect sm.state == candidate
	expect Send(VoteRequest to other peers) in actions.
}
func testAppend(sm *StateMachine) {
	sm = &SM {state = leader, log = {}..., commitIndex = ...., etc.}
	actions = sm.ProcessEvent(Append{data})
	look for Send{AppendEntries{}} in actions
	compare input data to data in AppendEntries.
}
*/
func testSimple(t *testing.T){
}
func testAppendEntriesReq(sm *StateMachine){
	//
}
func testAppendEntriesResp(sm *StateMachine){

}
func testVoteReq(sm *StateMachine){

}
func testVoteResp(sm *StateMachine){

}
/*
func getLeveledResponse(actions []Action, actionMap *map[string]bool, smMap map[int]StateMachine, level int){
	orig := level
	for ;level > 0;{
		if level != orig
	}
	for action := range actions {
		switch action.(type){
		case Send:
			actObj := action.(Send)
			resp_actions := smMap[actObj.peerId].ProcessEvent(3, actObj.event)
		case Commit:
			actObj := action.(Commit)
			actionMap[fmt.Sprintf("%v", actObj)] = true
		case LogStore:
			actObj := action.(LogStore)
			actionMap[fmt.Sprintf("%v", actObj)] = true
		case Alarm:
			actObj := action.(LogStore)
			actionMap[fmt.Sprintf("%v", actObj)] = true
		}
	}

}
func TestSimple(t *testing.T){
	sm1 := NewSm(FOLLOWER, 1, []int{2,3})
	sm2 := NewSm(FOLLOWER, 2, []int{1,3})
	sm3 := NewSm(LEADER, 3, []int{1,2})
	smMap := make(map[int]StateMachine)
	smMap[1] = sm1
	smMap[2] = sm2
	smMap[3] = sm3
	actionMap := make([string]bool)
	//msg := Msg{fromId:2,toId:1, event:Append{term:1, leaderId:2, prevLogTerm:1, prevLogIndex:1, entries:_entries,leaderCommit:1} }
	actions := sm3.ProcessEvent(0, AppendEv{data:[]byte{"first"}})
}
*/
