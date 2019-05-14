// Copyright (c) 2019 ELEME, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"flag"
	"fmt"
	"ekvproxy/proxy/log"
	"strings"
	"runtime"
	"ekvproxy/proxy/structure"
	"github.com/pingcap/tidb/store/tikv"
	"ekvproxy/proxy/util"
)

var (
	pdAddr     = flag.String("pd", "127.0.0.1:2379", "pd address,default:localhost:2379")
	logPath    = flag.String("lp", "", "log file path, if empty, default:stdout")
	logLevel   = flag.String("ll", "info", "log level:INFO|WARN|ERROR default:INFO")
	concurrent = flag.Int("cn", 0, "concurrent counts")
)

const (
	CONCURRENT = 50
)

func main() {

	flag.Parse()
	defer func() {
		if msg := recover(); msg != nil {
			log.Errorf("Panic: %v\n", msg)
		}
	}()

	initlog()

	log.Info("pdAddr:", *pdAddr)
	log.Info("logpath:", *logPath)
	log.Info("logpaht:", *logLevel)

	driver := tikv.Driver{}
	store, err := driver.Open(fmt.Sprintf("tikv://%s?cluster=1", *pdAddr))
	if err != nil {
		log.Fatal(err)
	}
	runtime.GOMAXPROCS(runtime.NumCPU())

	prefix := []byte{0x00}
	txn, err := store.Begin()
	iter, err := txn.Seek(nil)
	if err != nil {
		log.Errorf("concurrent seek regionid error:%s", err.Error())
	}
	var count_now uint64 = 0
	for iter.Valid() {

		tp, key, _ := structure.DecodeMetaKey(prefix, iter.Key())
		log.Infof("seek key [%s]", key)
		if structure.TypeFlag(tp) == structure.MetaCode {
			_, expireAt, _ := structure.DecodeMetaValue(iter.Value())
			if util.IsExpired(expireAt) {
				log.Infof("concurrent need delete: [%s]", key)
			}
		}

		count_now += 1
		//log.Infof("chunk ..... seek [%d] keys", count_now)
		err = iter.Next()
		if err != nil {
			log.Errorf("concurrent seek regionid error:%s", err.Error())
			break
		}
	}
	iter.Close()

	txn.Commit()

	log.Infof("******main seek [%d]******", count_now)
}

func initlog() {
	if len(*logPath) > 0 {
		log.SetHighlighting(false)
		err := log.SetOutputByName(*logPath)
		if err != nil {
			log.Fatalf("set log name failed - %s", err)
		}
	}

	switch strings.ToUpper(*logLevel) {
	case "INFO":
		log.SetLevel(log.LOG_LEVEL_INFO)
	case "ERROR":
		log.SetLevel(log.LOG_LEVEL_ERROR)
	case "WARN":
		log.SetLevel(log.LOG_LEVEL_WARN)
	default:
		log.SetLevel(log.LOG_LEVEL_INFO)
	}

	log.SetRotateByDay()
}
