// core implements the API layer (eg. Reply/Args)
// and the exposed executor method
package core

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
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
	Filename  string
	Partition uint32
	Data      []byte // raw data
}

// This could be replaced by the use of the reflect
// package (e.g, `reflect.ValueOf(func).Pointer()).Name()`).
const RemoteExecute = "RemoteExecutor.Execute"
const RemoteStow = "RemoteExecutor.Stow"

// RemoteExecutor holds the methods to be exposed by the RPC
// server as well as properties that modify the methods'
// behavior.
// All RPC calls are is the exported methods that a RPC client can
// make use of by calling the RPC server
//
// It takes a args and writes to pointer Reply if no error
type RemoteExecutor struct {
	StoragePath string
}

// Dummy map job
func (e *RemoteExecutor) Execute(args Args, reply *Reply) (err error) {
	log.Printf("Executing Job %s", args.Name)
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
	reply.Message = "ran:map_job"
	reply.DataLength = len(args.Data)

	return nil
}

// Stow data partitions
func (e *RemoteExecutor) Stow(args StowArgs, reply *Reply) (err error) {
	log.Printf("Stowing Partitioned Data")
	// do I stow the data in the worker nodes while
	// the data is being processed?
	// 1. include partition
	// 2. mount FS volumn in docker-compose
	err = ioutil.WriteFile(fmt.Sprintf(
		"./tmp/%s.part%03d", args.Filename, args.Partition), args.Data, 0644,
	)
	reply.DataLength = len(args.Data)
	reply.Message = "write:partitioned_data"

	return err
}

// handle interrupts from os in background goroutine
func HandleInterrupt() {
	signals := make(chan os.Signal, 1)

	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	log.Println("interrupt received")
}
