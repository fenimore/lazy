package main

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"

	"github.com/fenimore/lazy/core"
	"github.com/fenimore/lazy/executor"
	"github.com/fenimore/lazy/lazy"
)

type mapper struct{}

func (m mapper) MapRow(row []byte) ([]byte, []byte) {
	return row[:3], row[4:9]
}

type node struct {
	host string
	port int
}

func main() {
	//var setupCluster = flag.Bool("workers", false, "activates workers")
	//numWorkers = *flag.Int("num", 4, "number of executors")
	//flag.Parse()
	//log.Println(*setupCluster)

	possibleNodes := []node{
		node{"127.0.0.1", 7074},
		node{"127.0.0.1", 7073},
		node{"127.0.0.1", 7072},
		node{"127.0.0.1", 7071},
	} // distributed... ports
	log.Printf("Connecting to cluster of workers %v", possibleNodes)
	var network = make([]*executor.Executor, 0)

	for _, n := range possibleNodes {
		executor := &executor.Executor{Host: n.host, Port: n.port}

		err := executor.Connect()
		defer executor.Close()
		if err != nil {
			log.Println(err)
			continue
		}
		network = append(network, executor)
	}
	log.Printf("Connected to %d nodes out of %d possible", len(network), len(possibleNodes))

	// TODO: move this into, context readText or whatever
	logs, err := os.Open("data/flight_edges.tsv")
	if err != nil {
		log.Printf("Error Opening File: %s", err)
	}
	defer logs.Close()
	lineCount, err := lineCounter(logs)
	if err != nil {
		log.Printf("Error Getting Line Count: %s", err)
	}
	logs, err = os.Open("data/flight_edges.tsv")
	if err != nil {
		log.Printf("Error Opening File: %s", err)
	}
	linesPerPartition := lineCount / len(network)
	scanner := bufio.NewScanner(logs)
	partitions := make([]lazy.Partition, 0)
	pairs := make([]lazy.Pair, 0)
	index := uint16(0)
	for scanner.Scan() {
		line := scanner.Text()
		pairs = append(pairs, lazy.Pair{line, 1})
		if len(pairs) > linesPerPartition {
			partitions = append(partitions, lazy.Partition{Index: index, Data: pairs})
		}
	}

	networkId := 0
	ctx := new(lazy.Context)
	ctx.RunTask = func(rdd lazy.LazyRDD, part lazy.Partition, fn lazy.MapFunction) []lazy.Pair {
		reply, err := network[networkId].Execute(rdd, part, fn)
		if err != nil {
			log.Printf("Error with RPC %s", err)
		}
		networkId++
		if networkId == len(network) {
			networkId = 0
		}
		return reply.Results
	}
	rdd := lazy.NewRDD(ctx, partitions)
	_ = rdd.Collect()

	core.HandleInterrupt()
}

func lineCounter(r io.Reader) (int, error) {
	// https://stackoverflow.com/questions/24562942/golang-how-do-i-determine-the-number-of-lines-in-a-file-efficiently
	buf := make([]byte, 64*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}
