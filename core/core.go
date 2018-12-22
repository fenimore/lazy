// core implements the API layer (eg. Reply/Args)
// and the exposed executor method
package core

import (
	"bufio"
	"bytes"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type Reply struct {
	Message    string
	Ok         bool
	DataLength int
}

type Args struct {
	Name      string
	Partition uint32
	Data      []byte
}

type StowArgs struct {
	Name      string
	Partition uint32
	Data      []byte // raw data
}

// HandlerName provider the name of the only
// method that `core` exposes via the RPC
// interface.
//
// This could be replaced by the use of the reflect
// package (e.g, `reflect.ValueOf(func).Pointer()).Name()`).
const RemoteExecute = "RemoteExecutor.Execute"
const RemoteStow = "RemoteExecutor.Stow"

// Handler holds the methods to be exposed by the RPC
// server as well as properties that modify the methods'
// behavior.
type RemoteExecutor struct {
	StoragePath string
}

// Execute is the exported method that a RPC client can
// make use of by calling the RPC server
//
// It takes a args and writes to pointer Reply if no error
func (e *RemoteExecutor) Execute(args Args, reply *Reply) (err error) {
	log.Printf("Executing %s", args.Name)
	if args.Name == "" {
		err = errors.New("A name must be specified")
		return
	}

	scanner := bufio.NewScanner(bytes.NewReader(args.Data))

	result := make([]byte, 0)
	for scanner.Scan() {
		row := scanner.Bytes()
		// map rows
		// k, v := args.Map.MapRow(row)
		result = append(result, row...)
	}

	reply.Ok = true
	reply.Message = args.Name
	reply.DataLength = len(args.Data)

	return nil
}

func (e *RemoteExecutor) Stow(args StowArgs, reply *Reply) (err error) {
	log.Printf("Stowing Partitioned Data")
	// do I stow the data in the worker nodes while
	// the data is being processed?
	err = ioutil.WriteFile("./tmp/lazy_partition.part", args.Data, 0644)

	return err
}

// handle interrupts from os in background goroutine
func HandleInterrupt() {
	signals := make(chan os.Signal, 1)

	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	log.Println("interrupt received")
}
