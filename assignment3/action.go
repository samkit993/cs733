package main
import (
	"fmt"
)
//4 Actions
type Action interface {
	String() string
}
/*
func (action Action) String() string{
	switch action.(type){
	case Send:
		actObj := action.(Send)
		return fmt.Sprintf("%v", actObj)
	case Commit:
		actObj := action.(Commit)
		return fmt.Sprintf("%v", actObj)
	case Alarm:
		actObj := action.(Alarm)
		return fmt.Sprintf("%v", actObj)
	case LogStore:
		actObj := action.(LogStore)
		return fmt.Sprintf("%v", actObj)
	default:
		return fmt.Sprintf("Invalid action")
	}
}
*/
type Send struct {
	PeerId int
	Event  Event
}
func (s Send) String() string{
	return fmt.Sprintf("Send:peerId(%v) event(%v)",s.PeerId, s.Event)
}

type Commit struct {
	index int
	data []byte
	err error
}
func (c Commit) String() string{
	return fmt.Sprintf("Commit:idx(%v) data(%v) error(%v)",c.index, string(c.data), c.err)
}

type Alarm struct {
	duration int		//Time.Miliseconds
}
func (a Alarm) String() string{
	return fmt.Sprintf("Alarm:duration(%v)",a.duration)
}

type LogStore struct {
	index int
	data []byte
}

func (l LogStore) String() string{
	return fmt.Sprintf("LogStore:idx(%v) data(%v)",l.index, string(l.data))
}

type StateStore struct {
	currTerm int
	votedFor int
	log		 Log
}

func (s StateStore) String() string{
	return fmt.Sprintf("StateStore:currTerm(%v) votedFor(%v) log(%v)", s.currTerm, s.votedFor, s.log.String())
}
