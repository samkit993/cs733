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
	FromId       int
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Updates      LogUpdate
	LeaderCommit int
}
func (aer AppendEntriesReqEv) String() string{
	return fmt.Sprintf("AppendEntriesReqEv:fromId(%d) term(%v) leaderId(%v) prevLogIndex(%v) prevLogTerm(%v) entries(%v) leaderCommit(%v)",aer.FromId, aer.Term, aer.LeaderId, aer.PrevLogIndex, aer.PrevLogTerm, aer.Updates.String(), aer.LeaderCommit)
}

type AppendEntriesRespEv struct {
	FromId  int
	Term    int
	Success bool
}
func (aer AppendEntriesRespEv) String() string{
	return fmt.Sprintf("AppendEntriesRespEv:fromId(%v) term(%v) success(%v)",aer.FromId, aer.Term, aer.Success)
}

type VoteReqEv struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}
func (vr VoteReqEv) String() string{
	return fmt.Sprintf("VoteReqEv:term(%v) cadidateId(%v) lastLogIndex(%v) lastLogTerm(%v)", vr.Term, vr.CandidateId, vr.LastLogIndex, vr.LastLogTerm)
}

type VoteRespEv struct {
	FromId      int
	Term        int
	VoteGranted bool
}
func (vr VoteRespEv) String() string{
	return fmt.Sprintf("VoteRespEv:fromId(%v) term(%v) voteGranted(%v)",vr.FromId, vr.Term, vr.VoteGranted)
}

type AppendEv struct {
	Data []byte
}
func (ae AppendEv) String() string{
	return fmt.Sprintf("AppendEv:data(%v)", string(ae.Data))
}
