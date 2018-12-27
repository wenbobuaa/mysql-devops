package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
)

var (
	logFileName     = "binlog_sync.out"
	confName        = "./config.json"
	channelCapacity = 10240

	logFile  *os.File
	shellLog *log.Logger

	mainWG *sync.WaitGroup
)

func main() {

	// set log
	var err error
	logFile, err = os.Create(logFileName)
	if err != nil {
		panic(fmt.Sprintf("create log file failed: %v\n", err))
	}
	defer logFile.Close()

	shellLog = log.New(os.Stdout, "", 0)

	// read config
	var confList = make(map[string]*Config)
	err = unmarshalYAML(confName, confList)
	if err != nil {
		fmt.Printf("read config file failed: %v\n", err)
		return
	}

	controller := NewController()

	for w, conf := range confList {
		syncer := NewBinlogSyncer(conf)
		controller.AddWorker(&Worker{
			name:   w,
			syncer: syncer,
			config: conf,
		})
		go syncer.Sync()
	}

	controller.Listen("8888")
}

func init() {
	flag.StringVar(&logFileName, "log", logFileName, "file name to output error log")
	flag.StringVar(&confName, "config", confName, "configration file path")

	flag.Parse()
}
