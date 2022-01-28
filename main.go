package main

import (
	"github.com/bisegni/mqtt-test/cmd"
)

func main() {
	//runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	cmd.Execute()
}
