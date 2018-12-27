package lazy

import (
	"reflect"
	"sort"
	"testing"
)

func TestParallelizeSplitsEvenly(t *testing.T) {
	testData := []Pair{
		Pair{"a", 1},
		Pair{"b", 2},
		Pair{"c", 3},
		Pair{"d", 4},
	}

	ctx := new(Context)
	ctx.RunTask = ExecuteTaskLocally
	rdd := ctx.Parallelize(testData, 4)
	if len(rdd.partitions) != 4 {
		t.Error("Should be one partition per row")
	}
}

func TestParallelizesOnePartition(t *testing.T) {
	testData := []Pair{
		Pair{"a", 1},
		Pair{"b", 2},
		Pair{"c", 3},
		Pair{"d", 4},
	}

	ctx := new(Context)
	rdd := ctx.Parallelize(testData, 1)
	if len(rdd.partitions) != 1 {
		t.Error("Should be one partition")
	}
}

func TestRunJobCount(t *testing.T) {
	testData := []Pair{
		Pair{"a", 1},
		Pair{"b", 2},
		Pair{"c", 3},
		Pair{"d", 4},
	}
	ctx := new(Context)
	rdd := ctx.Parallelize(testData, 4)

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

	if ctx.RunJob(rdd, mapper, handler).integer != 4 {
		t.Error("Count should have been 4")
	}
}

func TestRunJobCollect(t *testing.T) {
	testData := []Pair{
		Pair{"a", 1},
		Pair{"b", 2},
		Pair{"c", 3},
		Pair{"d", 4},
	}
	ctx := new(Context)
	ctx.RunTask = ExecuteTaskLocally
	rdd := ctx.Parallelize(testData, 4)
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

	actual := ctx.RunJob(rdd, mapper, handler).list
	sort.Slice(actual, func(i, j int) bool {
		return actual[i].Val < actual[j].Val
	})
	if !reflect.DeepEqual(actual, testData) {
		t.Error("Should just return the same RDD pairs")
	}
}
