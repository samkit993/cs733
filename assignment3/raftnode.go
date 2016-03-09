package main
import (
	"fmt"
	"io/ioutil"
	"encoding/json"
)

type RaftNode struct{
	eventCh chan Event
	timeoutCh chan TimeoutEv
	sm StateMachine
}

type Node interface {
	Append([]byte)

	//A channel for client to listen on. What goes into Append must come out of here at some point
	CommitChannel() <- chan CommitInfo

	//Last known committed index in the log. This could be -1 untill the system stabilizes
	CommitedIndex() int

	//Returns the data at a log index or an error
	Get(index int) (err error, data []byte)

	//Node's id
	Id()

	//Id of leader,-1 if unknown
	LeaderId() int

	//Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers
	Shutdown()
}

type CommitInfo struct{
	Data []byte
	Index int64
	Err error
}


type Config struct{
	cluster []struct{
		Id  int
		Host string
		Port int
	}
	Id int
	LogDir string
	ElectionTimeout int
	HeartbeatTimeout int
}

type NetConfig struct{
	Id int
	Host string
	Port int
}
func checkError(e error){
	if e != nil {
		panic(e)
	}
}

//Thinking of taking input specification as json
//Having problems parsing nested JSON
func parseConfig(fname string) Config{
	nc1 := NetConfig{Id:1, Host:"host1", Port:8808}
	nc2 := NetConfig{Id:2, Host:"host2", Port:8808}
	
	ncarr := []NetConfig{}
	ncarr = append(ncarr, nc1)
	ncarr = append(ncarr, nc2)
	data, err := ioutil.ReadFile(fname)
	checkError(err)
	var config Config
	err = json.Unmarshal([]byte(data), &config)
	checkError(err)
	fmt.Println(config)
	//config := Config{

	encoded := `{
	"cluster": [{
		"Id": 1,
		"Host": "host1",
		"Port": 8818
	}, {
		"Id": 2,
		"Host": "host2",
		"Port": 8818
	}],
	"Id": 1,
	"LogDir": "log",
	"ElectionTimeout": 20,
	"HeartBeatTimeout": 15
}`
	config1 := Config{}
	err1 := json.Unmarshal([]byte(encoded), &config1)
	checkError(err1)
	fmt.Println(config1.cluster)
	return config
}

func (rn *RaftNode) doActions(actions []Action) {
}

func (rn *RaftNode) Append(data []byte) {
	rn.eventCh <- AppendEv{data: data}
}

func (rn *RaftNode) processEvents() {
	for {
		var ev Event
		select {
			case ev := <- rn.eventCh:
				fmt.Println("Got a event:", ev)	
			case to := <- rn.timeoutCh :
				fmt.Println("Got a timeout", to)	
			default:
				fmt.Println("In default")
		}
		actions := rn.sm.processEvent(1,ev) //How to get fromId //TODO
		rn.doActions(actions)
	}
}

func main(){
	parseConfig("input_specification.json")
}
