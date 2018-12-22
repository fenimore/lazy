package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"

	"github.com/fenimore/lazy/core"
)

type Worker struct {
	Host        string
	Port        int
	StoragePath string
	listener    net.Listener // private listener
}

func (w *Worker) Close() error {
	if w.listener != nil {
		return w.listener.Close()
	}
	return nil
}

func (w *Worker) Work() (err error) {
	rpc.Register(&core.RemoteExecutor{
		StoragePath: w.StoragePath,
	})

	w.listener, err = net.Listen("tcp", ":"+strconv.Itoa(w.Port))
	if err != nil {
		return err
	}

	log.Printf("Accepting Work on Host %s Port %d", w.Host, w.Port)
	rpc.Accept(w.listener)
	log.Printf("Stopping Work on Host %s Port %d", w.Host, w.Port)

	return nil
}

func main() {
	port := flag.Int("port", 7070, "port")
	flag.Parse()

	log.Printf("Starting Worker at port %d", *port)
	worker := &Worker{
		Host:        "127.0.0.1",
		Port:        *port,
		StoragePath: "./tmp/",
	}
	defer worker.Close()
	go func() {
		core.HandleInterrupt()
		log.Printf("Worker Disconnect %d", worker.Port)
		worker.Close()
		os.Exit(0)
	}()

	err := worker.Work()
	if err != nil {
		log.Panicln(err)
	}
}
