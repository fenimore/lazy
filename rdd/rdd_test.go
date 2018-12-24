package lazy

import (
	"fmt"
	"testing"
)

func TestMapRDD(t *testing.T) {
	testData := []Pair{
		Pair{"a", 1},
		Pair{"b", 2},
		Pair{"c", 3},
		Pair{"d", 4},
		// Pair{[]byte("a"), []byte("1")},
		// Pair{[]byte("b"), []byte("3")},
		// Pair{[]byte("c"), []byte("4")},
		// Pair{[]byte("d"), []byte("2")},
	}

	ctx := new(Context)
	rdd := ctx.Parallelize(testData, 4)
	mappedRDD := rdd.mapFunc(func(row Pair) Pair {
		fmt.Println(row.Val)
		fmt.Println(row.Val + 1)
		return Pair{
			row.Key,
			row.Val + 1,
		}
	})

	fmt.Println(rdd)
	fmt.Println(mappedRDD)
	fmt.Println(mappedRDD.collect())

	if len(mappedRDD.partitions()) != 4 {
		t.Error("Should be one partition per row")
	}
}
