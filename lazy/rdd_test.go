package lazy

import (
	"reflect"
	"testing"
)

func TestRDDCount(t *testing.T) {
	testData := []Pair{
		Pair{"a", 1},
		Pair{"b", 2},
		Pair{"c", 3},
		Pair{"d", 4},
	}

	ctx := new(Context)
	rdd := ctx.Parallelize(testData, 4)
	if rdd.count() != 4 {
		t.Error()
	}
}

func TestRDDCollect(t *testing.T) {
	testData := []Pair{
		Pair{"a", 1},
		Pair{"b", 2},
		Pair{"c", 3},
		Pair{"d", 4},
	}

	ctx := new(Context)
	rdd := ctx.Parallelize(testData, 4)

	actual := make([]Pair, 0)
	for _, pair := range rdd.collect() {
		actual = append(actual, pair)
	}
	if !reflect.DeepEqual(testData, actual) {
		t.Error("Map didn't process rdd")
	}
}

// Test MapRDD

func TestMapRDDComputes(t *testing.T) {
	testData := []Pair{
		Pair{"a", 1},
		Pair{"b", 2},
		Pair{"c", 3},
		Pair{"d", 4},
	}

	ctx := new(Context)
	rdd := ctx.Parallelize(testData, 4)
	mappedRDD := rdd.mapFunc(func(row Pair) Pair {
		return Pair{
			row.Key,
			row.Val + 1,
		}
	})

	if len(mappedRDD.partitions()) != 4 {
		t.Error("Should be one partition per row")
	}

	expected := []Pair{
		Pair{"a", 2},
		Pair{"b", 3},
		Pair{"c", 4},
		Pair{"d", 5},
	}

	actual := make([]Pair, 0)
	for _, part := range mappedRDD.partitions() {
		pairs := mappedRDD.compute(part)
		actual = append(actual, pairs...)
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Error("Map didn't process rdd")
	}
}

func TestMapRDDCount(t *testing.T) {
	testData := []Pair{
		Pair{"a", 1},
		Pair{"b", 2},
		Pair{"c", 3},
		Pair{"d", 4},
	}

	ctx := new(Context)
	rdd := ctx.Parallelize(testData, 4)
	mappedRDD := rdd.mapFunc(func(row Pair) Pair {
		return Pair{
			row.Key,
			row.Val + 1,
		}
	})

	if mappedRDD.count() != 4 {
		t.Error()
	}
}

func TestMapRDDCollect(t *testing.T) {
	testData := []Pair{
		Pair{"a", 1},
		Pair{"b", 2},
		Pair{"c", 3},
		Pair{"d", 4},
	}

	ctx := new(Context)
	rdd := ctx.Parallelize(testData, 4)
	mappedRDD := rdd.mapFunc(func(row Pair) Pair {
		return Pair{
			row.Key,
			row.Val,
		}
	})

	actual := make([]Pair, 0)
	for _, pair := range mappedRDD.collect() {
		actual = append(actual, pair)
	}
	if !reflect.DeepEqual(testData, actual) {
		t.Error("Map didn't process rdd")
	}
}

func TestChainMapRDD(t *testing.T) {
	mapper := func(row Pair) Pair {
		return Pair{
			row.Key,
			row.Val + 1,
		}
	}

	testData := []Pair{
		Pair{"a", 1},
		Pair{"b", 2},
		Pair{"c", 3},
		Pair{"d", 4},
	}

	ctx := new(Context)
	rdd := ctx.Parallelize(testData, 1)
	mappedRDD := rdd.mapFunc(mapper).mapFunc(mapper)

	expected := []Pair{
		Pair{"a", 3},
		Pair{"b", 4},
		Pair{"c", 5},
		Pair{"d", 6},
	}

	actual := make([]Pair, 0)
	for _, part := range mappedRDD.partitions() {
		pairs := mappedRDD.compute(part)
		actual = append(actual, pairs...)
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Error("Map didn't process rdd")
	}
}
