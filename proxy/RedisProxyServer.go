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
	"os"
	"flag"
	"fmt"
	"ekvproxy/proxy/handler"
	"ekvproxy/proxy/redis"
	"ekvproxy/proxy/log"
	"github.com/pingcap/tidb/store/tikv"
	"strings"
	"ekvproxy/proxy/config"
	"net/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"strconv"
	"ekvproxy/proxy/prometheus"
)

var (
	Version        = ""
	GitHash        = ""
	BuildTime      = ""
	versionflag    = flag.Bool("v", false, "Version")
	serverPort     = flag.Int("port", 6379, "listen port,default: 6379")
	confPath       = flag.String("conf", "conf.yaml", "config file of proxy")
	pdAddr         = flag.String("pd", "127.0.0.1:2379", "pd address,default:localhost:2379")
	logPath        = flag.String("lp", "", "log file path, if empty, default:stdout")
	logLevel       = flag.String("ll", "info", "log level:INFO|WARN|ERROR default:INFO")
	logMaxKeep     = flag.Uint("kpdays", 7, "keep log days for proxy")
	ignoreTTL      = flag.Bool("it", false, "ignore ttl when read,default false")
	connTimeout    = flag.Int("ct", 60*3, "connect timeout(s),default:180s")
	TimeOutData    = flag.Int("td", 1500, "request time out data")
	PrometheusPort = flag.Int("pp", 8080, "prometheus port,defalut port 8080")
)

func main() {
	flag.Parse()
	if *versionflag {
		fmt.Println("version: ", Version)
		fmt.Println("build time: ", BuildTime)
		fmt.Println("git version: ", GitHash)
		os.Exit(0)
	}
	config.ParseConf(confPath, &config.ConfigData)
	defer func() {
		if msg := recover(); msg != nil {
			log.Errorf("Panic: %v\n", msg)
		}
	}()

	initlog()
	prometheus.TimeOutThresholds = *TimeOutData
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Info("prometheus listen port", ":"+strconv.Itoa(*PrometheusPort))
		http.ListenAndServe(":"+strconv.Itoa(*PrometheusPort), nil)
	}()

	log.Info("serverPort:", *serverPort)
	log.Info("pdAddr:", *pdAddr)
	log.Info("logpath:", *logPath)
	log.Info("logpaht:", *logLevel)
	log.Info("ignoreTTL:", *ignoreTTL)
	log.Info("ConnTimeout:", *connTimeout)

	driver := tikv.Driver{}
	store, err := driver.Open(fmt.Sprintf("tikv://%s?disableGC=true", *pdAddr))
	if err != nil {
		log.Fatal(err)
	}
	myhandler := &handler.TxTikvHandler{store, []byte{0x00}, *ignoreTTL}

	srv, err := redis.NewServer(redis.DefaultConfig().Port(*serverPort).Handler(myhandler), *connTimeout)
	if err != nil {
		panic(err)
	}

	if err := srv.ListenAndServe(); err != nil {
		panic(err)
	}
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
	log.SetKeepAge(*logMaxKeep * 24)
	log.RotateDel()
}
