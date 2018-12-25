// client is how the leader interacts with a worker
package executor

import (
	"net/rpc"
	"strconv"

	"github.com/fenimore/lazy/core"
	"github.com/fenimore/lazy/lazy"
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

func (e *Executor) Execute(rdd lazy.RDD, part lazy.Partition, fn MapFunction) (*core.Reply, error) {
	var args = &core.Args{
		Name:      "Running Lazy job",
		RDD:       rdd,
		Partition: part,
		Mapper:    fn,
	}
	var reply = new(core.Reply)
	err := e.client.Call(core.RemoteRunJob, args, reply)

	return reply, err
}
