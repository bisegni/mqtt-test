package main

import (
	"runtime"

	"github.com/bisegni/mqtt-test/cmd"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	cmd.Execute()
}
