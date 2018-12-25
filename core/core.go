// core implements the API layer (eg. Reply/Args)
// and the exposed executor method
package core

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/fenimore/lazy/lazy"
)

type Reply struct {
	Results []lazy.Pair
	Message string
	Ok      bool
}

type Args struct {
	Name      string
	RDD       lazy.LazyRDD
	Partition lazy.Partition
	Mapper    lazy.MapFunction
	Handler   lazy.ResultsHandler
}

type StowArgs struct {
	Filename  string
	Partition uint32
	Data      []byte // raw data
}

// This could be replaced by the use of the reflect
// package (e.g, `reflect.ValueOf(func).Pointer()).Name()`).
const RemoteRunJob = "RemoteExecutor.RunJob"

// RemoteExecutor holds the methods to be exposed by the RPC
// server as well as properties that modify the methods'
// behavior.
// All RPC calls are is the exported methods that a RPC client can
// make use of by calling the RPC server
//
// It takes a args and writes to pointer Reply if no error
type RemoteExecutor struct {
	// should this be the spark context?
	StoragePath string
}

// Dummy map job
func (e *RemoteExecutor) RunJob(args Args, reply *Reply) (err error) {
	log.Printf("Executing Job %s", args.Name)

	results := make([]lazy.Pair, 0)
	for _, row := range rdd.compute(part) {
		results := append(results, fn(row))
	}

	reply.Results = results
	reply.Ok = true
	reply.Message = "ran:map_job"

	return nil
}

// handle interrupts from os in background goroutine
func HandleInterrupt() {
	signals := make(chan os.Signal, 1)

	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	log.Println("interrupt received")
}
