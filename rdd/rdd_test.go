package lazy

import (
	"reflect"
	"testing"
)

func TestMapRDD(t *testing.T) {
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
