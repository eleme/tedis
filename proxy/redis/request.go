// Copyright 2013 Docker, Inc.
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
//
// The following only applies to changes made to this file as part of ELEME development.
//
// Portions Copyright (c) 2019 ELEME, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.

package redis

import (
	"strconv"
)

type Request struct {
	Name       string
	Args       [][]byte
	Host       string
	ClientChan chan struct{}
}

func (r *Request) HasArgument(index int) bool {
	return len(r.Args) >= index+1
}

func (r *Request) ExpectArgument(index int) ReplyWriter {
	if !r.HasArgument(index) {
		return ErrNotEnoughArgs
	}
	return nil
}

func (r *Request) GetString(index int) (string, ReplyWriter) {
	if reply := r.ExpectArgument(index); reply != nil {
		return "", reply
	}
	return string(r.Args[index]), nil
}

func (r *Request) GetInteger(index int) (int, ReplyWriter) {
	if reply := r.ExpectArgument(index); reply != nil {
		return -1, reply
	}
	i, err := strconv.Atoi(string(r.Args[index]))
	if err != nil {
		return -1, ErrExpectInteger
	}
	return i, nil
}

func (r *Request) GetPositiveInteger(index int) (int, ReplyWriter) {
	i, reply := r.GetInteger(index)
	if reply != nil {
		return -1, reply
	}
	if i < 0 {
		return -1, ErrExpectPositivInteger
	}
	return i, nil
}

func (r *Request) GetStringSlice(index int) ([]string, ReplyWriter) {
	if reply := r.ExpectArgument(index); reply != nil {
		return nil, reply
	}
	var ret []string
	for _, elem := range r.Args[index:] {
		ret = append(ret, string(elem))
	}
	return ret, nil
}

func (r *Request) GetMap(index int) (map[string][]byte, ReplyWriter) {
	count := len(r.Args) - index
	if count <= 0 {
		return nil, ErrExpectMorePair
	}
	if count%2 != 0 {
		return nil, ErrExpectEvenPair
	}
	values := make(map[string][]byte)
	for i := index; i < len(r.Args); i += 2 {
		key, reply := r.GetString(i)
		if reply != nil {
			return nil, reply
		}
		values[key] = r.Args[i+1]
	}
	return values, nil
}
