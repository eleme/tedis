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
	"os"
	"ekvproxy/proxy/log"
	"github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/twinj/uuid"
	goctx "golang.org/x/net/context"
	"strings"
	"time"
	"github.com/pingcap/tidb/store/tikv/gcworker"
)

var (
	Version     = ""
	GitHash     = ""
	BuildTime   = ""
	versionflag = flag.Bool("v", false, "Version")
	pdAddr      = flag.String("pd", "127.0.0.1:2379", "pd address,default:localhost:2379")
)

func DoGc(pdservers string) error {
	driver := tikv.Driver{}
	store, err := driver.Open(fmt.Sprintf("tikv://%s?disableGC=true", pdservers))
	defer store.Close()
	if err != nil {
		log.Fatal(err)
	}

	etcdAddrs := strings.Split(pdservers, ",")
	pdCli, err := pd.NewClient(etcdAddrs)
	defer pdCli.Close()
	if err != nil {
		log.Fatal(err)
	}

	ctx := goctx.TODO()
	var sf uint64
	for {
		physicalts, logicalts, err := pdCli.GetTS(ctx)
		if err != nil {
			continue
		}
		cts := oracle.ComposeTS(physicalts, logicalts)
		physical := oracle.ExtractPhysical(cts)
		sec, nsec := physical/1e3, (physical%1e3)*1e6
		now := time.Unix(sec, nsec)

		safePoint := now.Add(-time.Minute * 10)
		sf = oracle.ComposeTS(oracle.GetPhysical(safePoint), 0)
		break;
	}
	kvstore := store.(tikv.Storage);
	return gcworker.RunGCJob(ctx, kvstore, sf, uuid.NewV4().String())
	//return tikv.RunGCJob(ctx, store, sf, uuid.NewV4().String())

}

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
	log.Info("pdAddr:", *pdAddr)
	DoGc(*pdAddr)
}
