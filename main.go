package main

import (
	"io"
	"log"
	"os"

	"github.com/fenimore/lazy/core"
	"github.com/fenimore/lazy/executor"
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

	nodes := make([]node, 0)
	possibleNodes := []node{
		node{"127.0.0.1", 7074},
		node{"127.0.0.1", 7073},
		node{"127.0.0.1", 7072},
		node{"127.0.0.1", 7071},
	} // distributed... ports

	log.Printf("Connecting to cluster of workers %v", nodes)

	// node keys shouldn't be pointers
	var network = make(map[node]*executor.Executor)

	for _, n := range possibleNodes {
		executor := &executor.Executor{Host: n.host, Port: n.port}
		defer executor.Close()
		err := executor.Connect()
		if err != nil {
			log.Println(err)
			continue
		}
		network[n] = executor
		nodes = append(nodes, n)
	}
	log.Printf("Connected to %d nodes out of %d possible", len(nodes), len(possibleNodes))
	logs, err := os.Open("data/flight_edges.tsv")
	if err != nil {
		log.Printf("Error Opening File: %s", err)
	}
	defer logs.Close()
	fi, err := logs.Stat()
	if err != nil {
		log.Printf("Error Stat File: %s", err)
	}

	var totalBytes = fi.Size()
	// TODO: optimize block length
	// and account for the remainder
	// and more than one partition per worker
	// Partitions are going to be automatically
	var maxPartitionSize = 1024 * 1024 * 64 // (64MB)
	var partitionCount = totalBytes / int64(len(nodes))
	log.Printf(
		"nodes: %d partitions %d tablesize: %d max: %d",
		len(nodes),
		partitionCount,
		totalBytes,
		maxPartitionSize,
	)

	// Partition the lines
	// can't use scan, because I need partitioned chunks

	idx := 0 // n is node index
	// I want to evenly distribute the partitions over the nodes
	for {
		buffer := make([]byte, partitionCount)
		n, err := logs.Read(buffer)
		if err == io.EOF {
			log.Printf("EOF with %d read", n)
			if n > 0 {
				// TODO: send last bit
			}
			break
		}

		worker := network[nodes[idx]]
		reply, err := worker.Stow("Stow Data", buffer)
		if err != nil {
			log.Printf("Scan error: %s", err)
		}
		log.Printf(
			"node: %d ok: %t len: %d result: %s",
			worker.Port,
			reply.Ok,
			reply.DataLength,
			reply.Message,
		)

		idx += 1 // n is the node index
		if idx >= len(nodes) {
			idx = 0
		}
	}

	core.HandleInterrupt()
}
