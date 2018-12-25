package lazy

import (
	"sync"

	"github.com/fenimore/lazy/executor"
)

type Context struct {
	lock *sync.Mutex
	// the nodes
	Nodes []*executor.Executor
	// retries
	// serializer
	// s3_conn
	// etc
}

type ResultHandler func([]Pair) Result

type Result struct {
	integer int
	list    []Pair
}

// princal gateawy into spark context from rdd
// the mapFunction passed in, is upposed to
func (ctx *Context) RunJob(rdd LazyRDD, fn MapFunction, hd ResultHandler) Result {
	// "this is the place to insert proper scheduler"
	// wg = new(sync.WaitGroup)
	// lock?
	results := make([]Pair, 0)
	if ctx.Nodes == nil {
		for _, partition := range rdd.partitions() {
			results = append(results, ExecuteTask(rdd, partition, fn))
		}
		final := hd(results)
		return final
	}

	nodeIndex := 0
	for _, partition := range rdd.partitions() {

		results = append(
			results,
			ctx.Nodes[nodeIndex].Execute(rdd, partition, fn),
		)

		nodeIndex += 1 // n is the node index
		if idx >= len(ctx.Nodes) {
			idx = 0
		}
	}

	final := hd(results)
	return final
}

// This is implemented in the
func ExecuteTaskLocally(rdd LazyRDD, part Partition, fn MapFunction) []Pair {
	results := make([]Pair, 0)
	for _, row := range rdd.compute(part) {
		results := append(results, fn(row))
	}
	return results
}

// TODO:
// textfile -> returns RDD
// s3_conn or hdfs_conn

func (ctx *Context) Parallelize(iterable []Pair, numPartitions int) RDD {
	var index uint16 = 0

	if numPartitions < 2 {
		partitions := []Partition{Partition{index, iterable}}
		return RDD{partitions, ctx}
	}

	partitions := make([]Partition, 0)
	lenData := len(iterable)
	for num := 0; num < numPartitions; num++ {
		start := num * lenData / numPartitions
		end := (num + 1) * lenData / numPartitions
		partitions = append(
			partitions,
			Partition{index, iterable[start:end]},
		)
		index++
	}

	return RDD{partitions, ctx}
}
