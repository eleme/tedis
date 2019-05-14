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

package util

import "bytes"

type MarkSet struct {
	one []byte
	set map[string]bool
}

func (s *MarkSet) Set(key []byte) {
	if s.one == nil {
		s.one = key
	} else {
		if s.set == nil {
			s.set = make(map[string]bool)
			s.set[string(s.one)] = true
		}
		s.set[string(key)] = true
	}
}

func (s *MarkSet) Len() int {
	if s.set != nil {
		return len(s.set)
	}
	if s.one != nil {
		return 1
	} else {
		return 0
	}
}

func (s *MarkSet) Has(key []byte) bool {
	if s.set != nil {
		return s.set[string(key)]
	}
	if s.one != nil {
		return bytes.Equal(key, s.one)
	} else {
		return false
	}
}
