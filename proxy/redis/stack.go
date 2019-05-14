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
	"sync"
)

type Stack struct {
	sync.Mutex
	Key   string
	stack [][]byte
	Chan  chan *Stack
}

func (s *Stack) PopBack() []byte {
	s.Lock()
	defer s.Unlock()

	if s.stack == nil || len(s.stack) == 0 {
		return nil
	}

	var ret []byte
	if len(s.stack)-1 == 0 {
		ret, s.stack = s.stack[0], [][]byte{}
	} else {
		ret, s.stack = s.stack[len(s.stack)-1], s.stack[:len(s.stack)-1]
	}
	return ret
}

func (s *Stack) PushBack(val []byte) {
	s.Lock()
	defer s.Unlock()
	if s.stack == nil {
		s.stack = [][]byte{}
	}

	go func() {
		if s.Chan != nil {
			s.Chan <- s
		}
	}()
	s.stack = append(s.stack, val)
}

func (s *Stack) PopFront() []byte {
	s.Lock()
	defer s.Unlock()

	if s.stack == nil || len(s.stack) == 0 {
		return nil
	}

	var ret []byte
	if len(s.stack)-1 == 0 {
		ret, s.stack = s.stack[0], [][]byte{}
	} else {
		ret, s.stack = s.stack[0], s.stack[1:]
	}
	return ret
}

func (s *Stack) PushFront(val []byte) {
	s.Lock()
	defer s.Unlock()

	if s.stack == nil {
		s.stack = [][]byte{}
	}

	s.stack = append([][]byte{val}, s.stack...)
	go func() {
		if s.Chan != nil {
			s.Chan <- s
		}
	}()
}

// GetIndex return the element at the requested index.
// If no element correspond, return nil.
func (s *Stack) GetIndex(index int) []byte {
	s.Lock()
	defer s.Unlock()

	if index < 0 {
		if len(s.stack)+index >= 0 {
			return s.stack[len(s.stack)+index]
		}
		return nil
	}
	if len(s.stack) > index {
		return s.stack[index]
	}
	return nil
}

func (s *Stack) Len() int {
	s.Lock()
	defer s.Unlock()
	return len(s.stack)
}

func NewStack(key string) *Stack {
	return &Stack{
		stack: [][]byte{},
		Chan:  make(chan *Stack),
		Key:   key,
	}
}
