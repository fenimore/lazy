// Taken from the org.apache.spark.RDD module
// **
//  * A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable,
//  * partitioned collection of elements that can be operated on in parallel. This class contains the
//  * basic operations available on all RDDs, such as `map`, `filter`, and `persist`. In addition,
//  * [[org.apache.spark.rdd.PairRDDFunctions]] contains operations available only on RDDs of key-value
//  * pairs, such as `groupByKey` and `join`;
//  * [[org.apache.spark.rdd.DoubleRDDFunctions]] contains operations available only on RDDs of
//  * Doubles; and
//  * [[org.apache.spark.rdd.SequenceFileRDDFunctions]] contains operations available on RDDs that
//  * can be saved as SequenceFiles.
//  * All operations are automatically available on any RDD of the right type (e.g. RDD[(Int, Int)])
//  * through implicit.
//  *
//  * Internally, each RDD is characterized by five main properties:
//  *
//  *  - A list of partitions
//  *  - A function for computing each split
//  *  - A list of dependencies on other RDDs
//  *  - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
//  *  - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
//  *    an HDFS file)
//  *
//  * All of the scheduling and execution in Spark is done based on these methods, allowing each RDD
//  * to implement its own way of computing itself. Indeed, users can implement custom RDDs (e.g. for
//  * reading data from a new storage system) by overriding these functions. Please refer to the
//  * <a href="http://people.csail.mit.edu/matei/papers/2012/nsdi_spark.pdf">Spark paper</a>
//  * for more details on RDD internals.
//  */
package lazy

type MapFunction func(Pair) Pair
type MapPartitionsFunction func(uint8, Partition) Partition

type Pair struct {
	// instead of pair, consider
	// type row interface
	Key string
	Val int
}

// "A partition (aka split) is a logical chunk of a large distributed data set."
type Partition struct {
	Index uint16
	Data  []Pair
}

type LazyRDD interface {
	// ala spec
	Partitions() []Partition
	//preferredLocations(Partition) []Node
	//dependencies() []Dependencies
	//partitioner() MetaData
	// nestpas?
	Compute(Partition) []Pair
}

//////////////
// Base RDD //
//////////////
// TODO: make a Hadoop, or split file RDD with partitions that were addresses

type RDD struct {
	partitions []Partition
	Context    *Context
}

func NewRDD(ctx *Context, parts []Partition) RDD {
	return RDD{partitions: parts, Context: ctx}
}

// Returns a partition after computing
// whatever it is your doing with the partition
func (rdd RDD) Compute(p Partition) []Pair {
	// so if this was a MapParititionRDD, the computer
	// would compute the parent RDD and then do func on this one
	return p.Data
}

func (rdd RDD) Partitions() []Partition {
	return rdd.partitions
}

// A basic transformation
func (rdd RDD) MapFunc(fn MapFunction) MapRDD {
	return MapRDD{rdd, fn, rdd.Context}
}

func (rdd RDD) Count() int {
	mapper := func(pair Pair) Pair {
		return pair
	}
	handler := func(pairs chan Pair) Result {
		count := 0
		for _ = range pairs {
			count++
		}
		return Result{integer: count}
	}

	result := rdd.Context.RunJob(rdd, mapper, handler)
	return result.integer
}

func (rdd RDD) Collect() []Pair {
	mapper := func(pair Pair) Pair {
		return pair
	}
	handler := func(ch chan Pair) Result {
		pairs := make([]Pair, 0)
		for pair := range ch {
			pairs = append(pairs, pair)
		}
		return Result{list: pairs}
	}

	result := rdd.Context.RunJob(rdd, mapper, handler)
	return result.list
}

///////////////////////
// MapPartitions RDD //
///////////////////////
type MapRDD struct {
	parent  LazyRDD
	fn      MapFunction
	context *Context
}

func (rdd MapRDD) Compute(p Partition) []Pair {
	results := make([]Pair, 0)
	parentData := rdd.parent.Compute(p)
	for _, v := range parentData {
		results = append(results, rdd.fn(v))
	}

	return results
}

func (rdd MapRDD) Partitions() []Partition {
	return rdd.parent.Partitions()
}

func (rdd MapRDD) MapFunc(fn MapFunction) MapRDD {
	return MapRDD{rdd, fn, rdd.context}
}

func (rdd MapRDD) Count() int {
	mapper := func(pair Pair) Pair {
		return pair
	}
	handler := func(ch chan Pair) Result {
		count := 0
		for _ = range ch {
			count++
		}
		return Result{integer: count}
	}

	result := rdd.context.RunJob(rdd, mapper, handler)
	return result.integer
}

func (rdd MapRDD) Collect() []Pair {
	mapper := func(pair Pair) Pair {
		return pair
	}
	handler := func(ch chan Pair) Result {
		pairs := make([]Pair, 0)
		for pair := range ch {
			pairs = append(pairs, pair)
		}
		return Result{list: pairs}
	}

	result := rdd.context.RunJob(rdd, mapper, handler)
	return result.list
}
