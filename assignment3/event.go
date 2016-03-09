package main
import (
	"fmt"
)

type Event interface {

}

//6 Events
type TimeoutEv struct{

}
func (to TimeoutEv) String() string{
	return fmt.Sprintf("TimeoutEv")
}

type AppendEntriesReqEv struct {
	term int
	leaderId int
	prevLogIndex int
	prevLogTerm int
	entries Log
	leaderCommit int
}
func (aer AppendEntriesReqEv) String() string{
	return fmt.Sprintf("AppendEntriesReqEv:term(%v) leaderId(%v) prevLogIndex(%v) prevLogTerm(%v) entries(%v) leaderCommit(%v)",aer.term, aer.leaderId, aer.prevLogIndex, aer.prevLogTerm, aer.entries.String(), aer.leaderCommit)
}

type AppendEntriesRespEv struct {
	term int
	success bool
}
func (aer AppendEntriesRespEv) String() string{
	return fmt.Sprintf("AppendEntriesRespEv:term(%v) success(%v)", aer.term, aer.success)
}

type VoteReqEv struct {
	term int
	candidateId int
	lastLogIndex int
	lastLogTerm int
}
func (vr VoteReqEv) String() string{
	return fmt.Sprintf("VoteReqEv:term(%v) cadidateId(%v) lastLogIndex(%v) lastLogTerm(%v)", vr.term, vr.candidateId, vr.lastLogIndex, vr.lastLogTerm)
}

type VoteRespEv struct {
	term int
	voteGranted bool
}
func (vr VoteRespEv) String() string{
	return fmt.Sprintf("VoteRespEv:term(%v) voteGranted(%v)", vr.term, vr.voteGranted)
}

type AppendEv struct {
	data []byte
}
func (ae AppendEv) String() string{
	return fmt.Sprintf("AppendEv:data(%v)", string(ae.data))
}
