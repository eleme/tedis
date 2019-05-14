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
	"encoding/json"
	"time"
	"fmt"
	"github.com/vmihailenco/msgpack"
	"compress/zlib"
	"bytes"
	"io"
)

func main() {

	oldMap := make(map[string][]byte)
	for i := 0; i < 100; i++ {
		key := "sdfasdfson" + string(i)
		oldMap[key] = []byte("abcdefghijklmnopqrstuvwxyz1234567890!@#$")
	}
	rets := make([][]byte, len(oldMap)*2)
	i := 0
	arr := make([]byte, 0)
	for k, v := range oldMap {
		rets[i*2], rets[i*2+1] = []byte(k), v
		i++
		arr = append(arr, []byte(k)...)
		arr = append(arr, v...)
	}
	b, _ := msgpack.Marshal(rets)
	fmt.Println(len(b))

	start := time.Now()
	for j := 0; j < 1000; j++ {
		compress(b)
	}
	fmt.Println(time.Since(start))

	cc := compress(b)
	fmt.Println(len(cc))
	start = time.Now()
	for j := 0; j < 1000; j++ {
		uncompress(cc)
	}
	fmt.Println(time.Since(start))

	cce := compress(cc)
	fmt.Println(len(cce))

	benchjson(oldMap, 1000)
	benchmsgpack(oldMap, 1000)
}

func benchmsgpack(oldMap map[string][]byte, count int) {
	b, _ := msgpack.Marshal(oldMap)
	fmt.Println(len(b))

	var newMap map[string][]byte
	msgpack.Unmarshal(b, &newMap)
	fmt.Println("msgpack:", newMap)

	start := time.Now()
	for j := 0; j < count; j++ {
		msgpack.Marshal(oldMap)
	}
	fmt.Println(time.Since(start))

	start = time.Now()
	for j := 0; j < count; j++ {
		var oldMap map[string][]byte
		msgpack.Unmarshal(b, &oldMap)
	}
	fmt.Println(time.Since(start))

	start = time.Now()
	for j := 0; j < count; j++ {
		rets := make([][]byte, len(oldMap)*2)
		i := 0
		for k, v := range oldMap {
			rets[i*2], rets[i*2+1] = []byte(k), v
			i++
		}
	}

	fmt.Println(time.Since(start))
}

func benchjson(oldMap map[string][]byte, count int) {
	start := time.Now()
	newJsonData, _ := json.Marshal(oldMap)
	fmt.Println(len(newJsonData))
	var newMap map[string][]byte
	json.Unmarshal(newJsonData, &newMap)
	fmt.Println("json:", newMap)

	for j := 0; j < count; j++ {
		json.Marshal(oldMap)
	}
	fmt.Println(time.Since(start))

	start = time.Now()
	for j := 0; j < count; j++ {
		var oldMap map[string][]byte
		json.Unmarshal(newJsonData, &oldMap)
	}
	fmt.Println(time.Since(start))

	start = time.Now()
	for j := 0; j < count; j++ {
		rets := make([][]byte, len(oldMap)*2)
		i := 0
		for k, v := range oldMap {
			rets[i*2], rets[i*2+1] = []byte(k), v
			i++
		}
	}

	fmt.Println(time.Since(start))
}

func compress(in []byte) []byte {
	var out bytes.Buffer
	w := zlib.NewWriter(&out)
	w.Write(in)
	w.Close()
	return out.Bytes()
}

func uncompress(in []byte) []byte {
	buf := bytes.NewBuffer(in)
	var out bytes.Buffer
	r, _ := zlib.NewReader(buf)
	io.Copy(&out, r)
	return out.Bytes()
}
