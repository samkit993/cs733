package main
func TestBasic(t *testing.T){
	reafts := makeRafts()
	ldr := getLeader(rafts)
	ldr.Append("foo")
	time.Sleep(1*time.Second)
	for _, node := rafts {
		select {
		case ci := <-node.CommitChannel():
			if ci.err != nil {
				t.Fatal(ci.err)
			}
			if string(ci.data) != "foo" {
				t.Fatal("Got different data")
			}
		default: t.Fatal("Expected message on all nodes")
		}
	}
}
