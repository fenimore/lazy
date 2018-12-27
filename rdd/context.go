package lazy

import (
	"sync"
)

// TODO; default constructor
type Context struct {
	lock    *sync.Mutex
	RunTask func(LazyRDD, Partition, MapFunction) []Pair
	// retries
	// serializer
	// s3_conn
	// etc
}

type ResultHandler func(chan Pair) Result

type Result struct {
	integer int
	list    []Pair
}

// princal gateawy into spark context from rdd
// the mapFunction passed in, is upposed to
func (ctx *Context) RunJob(rdd LazyRDD, fn MapFunction, hd ResultHandler) Result {
	wg := new(sync.WaitGroup)
	results := make(chan Pair, 1024) // needs big buffer
	for _, partition := range rdd.Partitions() {
		wg.Add(1)
		go ExecuteTaskGoroutine(results, wg, rdd, partition, fn)
	}
	wg.Wait()
	close(results)
	return hd(results)
}

// This is implemented in the
func ExecuteTaskGoroutine(ch chan Pair, wg *sync.WaitGroup, rdd LazyRDD, part Partition, fn MapFunction) {
	for _, row := range rdd.Compute(part) {
		ch <- fn(row)
	}
	wg.Done()
}

// This is implemented in the
func ExecuteTaskLocally(rdd LazyRDD, part Partition, fn MapFunction) []Pair {
	results := make([]Pair, 0)
	for _, row := range rdd.Compute(part) {
		results = appendb(results, fn(row))
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
