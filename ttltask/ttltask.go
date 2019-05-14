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
	"os"
	"fmt"
	"ekvproxy/proxy/log"
	"strings"
	"runtime"
	"ekvproxy/proxy/structure"
	"github.com/pingcap/tidb/store/tikv"
	//"github.com/pingcap/tidb/mysql"

	"ekvproxy/proxy/util"
	"ekvproxy/proxy/handler"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/pd/pd-client"
	"bytes"
	"github.com/juju/errors"
	"net/url"
	goctx "golang.org/x/net/context"
	"github.com/cznic/mathutil"
	"ekvproxy/ttltask/GoSlaves"
	"time"
	"ekvproxy/proxy/prometheus"
	"net/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"strconv"
)

var (
	Version                    = ""
	GitHash                    = ""
	BuildTime                  = ""
	g_count_seek_region uint64 = 0
	g_sleep_flag        bool   = false;
	g_count_consume     int    = 0

	versionflag = flag.Bool("v", false, "Version")
	flagConsume = flag.Int("fs", 1000, "when time consuming upper than [fs] ms, while pause running")
	pertConsume = flag.Int("pc", 10, "the percent of dels > flagConsume while low speed")

	pdAddr         = flag.String("pd", "10.101.65.68:2379", "pd address,default:localhost:2379")
	logPath        = flag.String("lp", "ttl.log", "log file path, if empty, default:stdout")
	logLevel       = flag.String("ll", "info", "log level:INFO|WARN|ERROR default:INFO")
	concurrent     = flag.Int("cn", 2, "concurrent counts")
	TimeOutData    = flag.Int("td", 1500, "request time out data")
	PrometheusPort = flag.Int("pp", 8080, "prometheus port,defalut port 8080")
)

const (
	CONCURRENT = 50
)

func main() {

	flag.Parse()
	if *versionflag {
		fmt.Println("version: ", Version)
		fmt.Println("build time: ", BuildTime)
		fmt.Println("git version: ", GitHash)
		os.Exit(0)
	}
	defer func() {
		if msg := recover(); msg != nil {
			log.Errorf("Panic: %v\n", msg)
		}
	}()
	prometheus.TimeOutThresholds = *TimeOutData
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Info("prometheus listen port", ":"+strconv.Itoa(*PrometheusPort))
		http.ListenAndServe(":"+strconv.Itoa(*PrometheusPort), nil)
	}()

	log.Info("pdAddr:", *pdAddr)
	log.Info("logpath:", *logPath)
	log.Info("logpaht:", *logLevel)

	driver := tikv.Driver{}
	store, err := driver.Open(fmt.Sprintf("tikv://%s?cluster=1", *pdAddr))
	if err != nil {
		log.Fatal(err)
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	pdcli, err := pd_client(fmt.Sprintf("tikv://%s?cluster=1", *pdAddr))
	if (err != nil) {
		log.Fatal(err)
	}
	defer pdcli.Close()

	rcache := tikv.NewRegionCache(pdcli)
	bo := tikv.NewBackoffer(5000, goctx.Background())
	regions, err := rcache.ListRegionIDsInKeyRange(bo, nil, nil)
	if (err != nil) {
		log.Fatal(err)
	}
	regionsCount := len(regions)
	log.Infof("regions count is [%d]", regionsCount)
	if *concurrent == 0 {
		*concurrent = CONCURRENT
	}
	chunk := regionsCount/(*concurrent) + 1
	chunkcn := regionsCount / chunk
	if (regionsCount%chunk > 0) {
		chunkcn++
	}

	myhandler := &handler.TxTikvHandler{store, []byte{0x00}, false}

	sp := slaves.MakePool(uint(*concurrent), func(obj interface{}) interface{} {
		b, _ := obj.([]byte)
		log.Infof("pool delete: [%s]", b)
		start := time.Now()
		myhandler.CheckExpireAndDel([][]byte{b})

		del_time := time.Since(start)
		prometheus.CmdHistogram.WithLabelValues("ttl_del").Observe(float64(del_time))
		if del_time > (time.Duration(*flagConsume) * time.Millisecond) {
			g_count_consume++
			for (g_count_consume > ((*concurrent) * (*pertConsume) / 100)) {
				time.Sleep(time.Duration(200) * time.Millisecond)
			}
			//log.Errorf("zcf----test--- ttl_del time: %d ms", del_time/time.Millisecond)
			return nil
		}
		g_count_consume --
		return nil
	}, nil)
	if err := sp.Open(); err != nil {
		panic(err)
	}
	defer sp.Close()
	log.Infof("threadpool nu:[%d]", len(sp.Slaves))

	var begin int = 0
	var tmp int = 0
	var i = 0
	//var waitgroup sync.WaitGroup
	arrchan := make(chan uint64, chunkcn)
	for ; begin < regionsCount; begin += chunk {
		//waitgroup.Add(1)
		tmp = mathutil.Min(begin+chunk, regionsCount)
		log.Info("main :[", begin, ":", tmp, "] {", regions[begin:tmp], "}")
		go ttl_task_chunk(&sp, store, rcache, regions[begin:tmp], arrchan)
		i++
	}
	log.Infof("main total [%d], chunkcn [%d]", i, chunkcn)
	//waitgroup.Wait()

	var totalcount uint64 = 0
	for j := 0; j < i; j++ {
		totalcount += <-arrchan
		//log.Infof("******roution seek [%d]******", totalcount)
	}
	log.Errorf("*********main finish regins [%d]*********", g_count_seek_region)

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
	log.SetKeepAge(3 * 24)
	log.RotateDel()
}

func parsePath(path string) (etcdAddrs []string, disableGC bool, err error) {
	var u *url.URL
	u, err = url.Parse(path)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	if strings.ToLower(u.Scheme) != "tikv" {
		err = errors.Errorf("Uri scheme expected[tikv] but found [%s]", u.Scheme)
		log.Error(err)
		return
	}
	switch strings.ToLower(u.Query().Get("disableGC")) {
	case "true":
		disableGC = true
	case "false", "":
	default:
		err = errors.New("disableGC flag should be true/false")
		return
	}
	etcdAddrs = strings.Split(u.Host, ",")
	return
}

func pd_client(path string) (pd.Client, error) {

	etcdAddrs, _, err := parsePath(path)
	if err != nil {
		return nil, err
	}

	pdCli, err := pd.NewClient(etcdAddrs)
	if err != nil {
		return nil, err
	}

	return pdCli, nil
}

func ttl_task_chunk(sp *slaves.SlavePool, store kv.Storage, rcache *tikv.RegionCache, regions []uint64, ch chan uint64) {
	var count_now uint64 = 0
	var count_del uint64 = 0
	var count_print uint64 = 0
	var count_error uint64 = 0
	defer func() {
		ch <- count_now
	}()

	log.Info("concurrent regions :{", regions, "}")
	prefix := []byte{0x00}

	bo := tikv.NewBackoffer(5000, goctx.Background())

	var id uint64
	var region *tikv.KeyLocation
	for _, id = range regions {
		txn, err := store.Begin()
		if err != nil {
			log.Fatal(err)
		}
		region, err = rcache.LocateRegionByID(bo, id)
		if (err != nil) {
			txn.Commit(goctx.Background())
			log.Errorf("concurrent LocationRegionById error:%s", err.Error())
			continue
		}
		g_count_seek_region++
		iter, err := txn.Seek(region.StartKey)
		if err != nil {
			log.Errorf("zcf----test---concurrent seek regionid [%d] error:%s", id, err.Error())
			count_error++
			iter.Close()
			txn.Commit(goctx.Background());
			txn, err = store.Begin()
			if err != nil {
				log.Fatal(err)
			}
			//break,
			iter, err = txn.Seek(region.StartKey)
			if err != nil {
				txn.Commit(goctx.Background())
				log.Errorf("concurrent seek regionid [%d] error:%s", id, err.Error())
				break;
			}
		}
		for iter.Valid() {
			if len(region.EndKey) != 0 && bytes.Compare(iter.Key(), region.EndKey) >= 0 {
				//log.Infof("concurrent region [%d] break key [%s]",id, iter.Key())
				break
			}
			prev_iter_key := iter.Key();
			tp, key, _ := structure.DecodeMetaKey(prefix, iter.Key())
			if structure.TypeFlag(tp) == structure.MetaCode {
				_, expireAt, _ := structure.DecodeMetaValue(iter.Value())
				if util.IsExpired(expireAt) {
					sp.SendWork(key)
					count_del++
				}
			}

			count_now += 1
			err = iter.Next()
			if err != nil {
				count_error++
				log.Errorf("zcf----test---concurrent seek regionid [%d] error:%s", id, err.Error())
				iter.Close()
				txn.Commit(goctx.Background())
				txn, err = store.Begin()
				if err != nil {
					log.Fatal(err)
				}
				iter, err = txn.Seek(prev_iter_key)
				if err != nil {
					log.Errorf("concurrent seek regionid [%d] error:%s", id, err.Error())
					break;
				}
			}
			if ((count_del - count_print) / 100000) > 0 {
				count_print = count_del
				runtime.GC()
				txn.Commit(goctx.Background())
				txn, err = store.Begin()
				if err != nil {
					log.Fatal(err)
				}
				iter, err = txn.Seek(prev_iter_key)
				if err != nil {
					log.Errorf("concurrent seek regionid [%d] error:%s", id, err.Error())
					break;
				}
				log.Errorf("zcf----test---now regionid [%d] delete: [%d]", id, count_now)
			}
			for sp.WorkQueueLen() > 10000 {
				time.Sleep(time.Duration(1) * time.Second)
			}
		}
		iter.Close()
		txn.Commit(goctx.Background())
		log.Infof("concurrent region finished [%d]", id)

	}
	log.Errorf("finished........... seek [%d] keys", count_del)
}
