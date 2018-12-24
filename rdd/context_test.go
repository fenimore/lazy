package lazy

import (
	"reflect"
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
	rdd := ctx.Parallelize(testData, 4)
	if len(rdd.Partitions) != 4 {
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
	if len(rdd.Partitions) != 1 {
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
	handler := func(pairs []Pair) Result {
		return Result{integer: len(pairs)}
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
	rdd := ctx.Parallelize(testData, 4)
	mapper := func(pair Pair) Pair {
		return pair
	}
	handler := func(pairs []Pair) Result {
		return Result{list: pairs}
	}

	if !reflect.DeepEqual(ctx.RunJob(rdd, mapper, handler).list, testData) {
		t.Error("Should just return the same RDD pairs")
	}
}
