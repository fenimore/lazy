package lazy

type Context struct {
	pool RDD
	// retries
	// serializer
	// s3_conn
	// etc
}

type resultHandler func([]Pair) Result

type Result struct {
	integer int
	list    []Pair
}

// princal gateawy into spark context from rdd
// the mapFunction passed in, is upposed to
func (ctx *Context) RunJob(rdd SparkRDD, fn mapFunction, hd resultHandler) Result {
	// acquire a lock

	// "this is the place to insert proper scheduler"
	//
	// First implement local
	// then implement distributed by building a serialized
	// task context which is sent to each executor

	// use a semaphore to compute each partition

	// local execution
	results := make([]Pair, 0)
	for _, partition := range rdd.partitions() {
		// for every partition (if distributed, do this RPC?
		for _, row := range rdd.compute(partition) {
			// compute the row and then do the map "action"
			results = append(results, fn(row))
		}
	}
	// release lock
	return hd(results)
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
