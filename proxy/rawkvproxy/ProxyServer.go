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
	"fmt"
	"flag"
	"ekvproxy/proxy/log"
	"github.com/pingcap/tidb/store/tikv"
	"strings"
	"ekvproxy/proxy/redis"
	"ekvproxy/proxy/config"
	"errors"
	"github.com/pingcap/tidb/util/codec"
	"ekvproxy/proxy/handler"
	"ekvproxy/proxy/util"
	"ekvproxy/proxy/prometheus"
	"net/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"strconv"
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
	connTimeout    = flag.Int("ct", 60*3, "connect timeout(s),default:180s")
	compression    = flag.Bool("cp", false, "compress the value")
	TimeOutData    = flag.Int("td", 1500, "request time out data")
	PrometheusPort = flag.Int("pp", 8080, "prometheus port,defalut port 8080")
)

type TikvHandler struct {
	client tikv.RawKVClient
}

// Get override the DefaultHandler's method.
func (h *TikvHandler) GET(key []byte) ([]byte, error) {
	log.Infof("get key:%s", key)
	ekey := codec.EncodeBytes(nil, key)
	res, err := h.client.Get([]byte(ekey))
	if err != nil {
		log.Error(err)
		return nil, err
	}
	len := len(res)
	if len == 0 {
		return res, nil
	}

	if len > 0 {
		prometheus.LengthHistogram.WithLabelValues("get").Observe(float64(len))
		prometheus.CmdCounter.WithLabelValues("get").Inc()
	}
	if *compression {
		result, err := util.Uncompress(res)
		if err == nil {
			return result, err

		}
	}

	return res, nil

}

func (h *TikvHandler) AUTH(pwd string) (error) {
	log.Infof("passwd [%s] , conf pwd [%s]", pwd, config.ConfigData.Passwd)
	//if pwd == config.ConfigData.Passwd {
	if pwd == "1234" {
		//session.Authed = true
		return nil
	} else {
		return errors.New("Invalid passwd")
	}
}

func (h *TikvHandler) NOAUTH() (error) {

	return errors.New("Invalid passwd")
}

func (h *TikvHandler) SET(key []byte, val []byte) error {
	log.Infof("set key:%s", key)
	ekey := codec.EncodeBytes(nil, key)
	if kerr := handler.CheckKeySize(key); kerr != nil {
		log.Errorf("invalid key: %s", key)
		return kerr
	}

	prometheus.LengthHistogram.WithLabelValues("set").Observe(float64(len(val)))

	if *compression {
		val = util.Compress(val);
		prometheus.LengthHistogram.WithLabelValues("compress_set").Observe(float64(len(val)))
	}

	err := h.client.Put(ekey, val)
	if err != nil {
		log.Error(err)
	}

	return err
}

func (h *TikvHandler) DEL(keys [][]byte) (int, error) {
	log.Infof("del keys:%s", keys)
	var err error
	for i, key := range keys {
		ekey := codec.EncodeBytes(nil, key)
		err = h.client.Delete(ekey)
		if err != nil {
			log.Error(err)
			return i, err
		}
	}

	return len(keys), err
}

func (h *TikvHandler) MGET(keys [][]byte) ([][]byte, error) {
	values := make([][]byte, len(keys))
	log.Infof("mget keys:%s", keys)

	for _, key := range keys {
		if kerr := handler.CheckKeySize(key); kerr != nil {
			return nil, kerr
		}
	}

	var ierr error
	for i, key := range keys {
		ekey := codec.EncodeBytes(nil, key)
		res, err := h.client.Get([]byte(ekey))
		if err != nil {
			ierr = err
			break
		}

		if *compression {
			if len(res) > 0 {
				result, err := util.Uncompress(res)
				if err == nil {
					res = result
				}
			}
		}
		values[i] = res

	}

	if ierr != nil {
		log.Error(ierr)
	}
	return values, ierr

}

func (h *TikvHandler) QUIT() ([]byte, error) {
	return []byte("OK"), nil
}

func (h *TikvHandler) INFO() ([]byte, error) {
	return []byte("version:rawkv1.0.1"), nil
}

func (h *TikvHandler) PING() ([]byte, error) {
	return []byte("PONG"), nil
}

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

	log.Info("serverPort:", *serverPort)
	log.Info("pdAddr:", *pdAddr)
	log.Info("logpath:", *logPath)
	log.Info("logpaht:", *logLevel)
	log.Info("ConnTimeout:", *connTimeout)
	log.Info("compression:", *compression)

	cli, err := tikv.NewRawKVClient(strings.Split(*pdAddr, ","))
	if err != nil {
		log.Fatal(err)
	}
	myhandler := &TikvHandler{*cli}

	prometheus.TimeOutThresholds = *TimeOutData
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Info("prometheus listen port", ":"+strconv.Itoa(*PrometheusPort))
		http.ListenAndServe(":"+strconv.Itoa(*PrometheusPort), nil)
	}()

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
