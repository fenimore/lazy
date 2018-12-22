// client is how the leader interacts with a worker
package executor

import (
	"net/rpc"
	"strconv"

	"github.com/fenimore/lazy/core"
)

type Executor struct {
	Port   int
	Host   string
	client *rpc.Client // private client for communicating with worker nodes
}

// Init initializes the underlying RPC client that is
// responsible for taking a codec and writing the RPC
// details down to it.
func (e *Executor) Connect() (err error) {
	addr := e.Host + ":" + strconv.Itoa(e.Port)

	e.client, err = rpc.Dial("tcp", addr)

	return err
}

func (e *Executor) Close() error {
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

func (e *Executor) Execute(name string, data []byte) (*core.Reply, error) {
	var args = &core.Args{Name: name, Data: data}
	var reply = new(core.Reply)
	err := e.client.Call(core.RemoteExecute, args, reply)

	return reply, err
}

func (e *Executor) Stow(name string, data []byte) (*core.Reply, error) {
	var args = &core.StowArgs{Name: name, Data: data}
	var reply = new(core.Reply)
	err := e.client.Call(core.RemoteStow, args, reply)

	return reply, err
}
