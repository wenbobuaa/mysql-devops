package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
)

type Worker struct {
	name   string
	syncer *BinlogSyncer
	config *Config
}

type Controller struct {
	WorkerList  map[string]*Worker
	workerNames []string
}

func NewController() *Controller {
	return &Controller{
		WorkerList: make(map[string]*Worker, 10),
	}
}

func (ctl Controller) AddWorker(w *Worker) {
	if ctl.WorkerList[w.name] == nil {
		fmt.Printf("add worker name to worker list")
		fmt.Printf("name: %v\n", w.name)

		ctl.workerNames = append(ctl.workerNames, w.name)
	}
	fmt.Printf("len: %v, names: %v\n", len(ctl.workerNames), ctl.workerNames)
	ctl.WorkerList[w.name] = w
}

func (ctl Controller) Listen(port string) {
	var addr string
	if port == "" {
		addr = "127.0.0.1:8888"
	} else {
		addr = "127.0.0.1:" + port
	}

	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Print("Listen failed: %v\n", err)
	}

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Printf("connection accept failed: %v\n", err)
			break
		}

		go ctl.handler(c)
	}
}

func (ctl Controller) handler(c net.Conn) {
	defer c.Close()

	var data bytes.Buffer

	_, err := io.Copy(&data, c)
	if err != nil {
	}

	cmd := strings.Fields(string(data.Bytes()))
	rst, err := ctl.execCmd(cmd)
	if err != nil {
		c.Write([]byte(fmt.Sprintf("failed to execute command: %v\n", err)))
	}

	c.Write([]byte(rst))
}

func (ctl Controller) execCmd(cmd []string) (string, error) {
	var worker []string
	fmt.Printf("workernames: %v\n", ctl.workerNames)
	if len(cmd[1:]) == 0 {
		worker = ctl.workerNames
	} else {
		worker = cmd[1:]
	}

	switch cmd[0] {
	case "stat":
		return ctl.execStat(worker)
	case "wake":
		return ctl.execWake(worker), nil
	case "stop":
		return ctl.execStop(worker), nil
	default:
		return "", errors.New(fmt.Sprintf("error command, should be one of [stat, wake, stop], got: %s\n", cmd[0]))
	}
}

func (ctl Controller) execStat(workers []string) (string, error) {
	rst := make(map[string]*Status, len(workers))

	fmt.Printf("worker list: %v\n", ctl.WorkerList)
	fmt.Printf("workers: %v\n", workers)
	for _, w := range workers {
		syncer := ctl.WorkerList[w].syncer
		if syncer != nil {
			rst[w] = syncer.GetStat()
		}
		rst[w] = nil
	}

	return marshalYAML(rst)
}

func (ctl Controller) execStop(workers []string) string {
	for _, w := range workers {
		syncer := ctl.WorkerList[w].syncer
		if syncer != nil {
			syncer.stop = true
		}
	}

	return "finish"
}

func (ctl Controller) execWake(workers []string) string {
	for _, w := range workers {
		worker := ctl.WorkerList[w]
		if worker != nil && worker.syncer.stop {
			syncer := NewBinlogSyncer(worker.config)
			worker.syncer = syncer
			go syncer.Sync()
		}
	}

	return "finish"
}
