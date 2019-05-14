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

import (
	"math"
	"fmt"
	"strconv"
	"github.com/juju/errors"
)

func Num64(i interface{}) interface{} {
	switch x := i.(type) {
	case int:
		return int64(x)
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return int64(x)
	case uint:
		return uint64(x)
	case uint8:
		return uint64(x)
	case uint16:
		return uint64(x)
	case uint32:
		return uint64(x)
	case uint64:
		return uint64(x)
	case float32:
		return float64(x)
	case float64:
		return float64(x)
	default:
		return x
	}
}

func ParseInt(i interface{}) (int64, error) {
	var s string
	switch x := Num64(i).(type) {
	case int64:
		return int64(x), nil
	case uint64:
		if x > math.MaxInt64 {
			return 0, errors.New("integer overflow")
		}
		return int64(x), nil
	case float64:
		switch {
		case math.IsNaN(x):
			return 0, errors.New("float is NaN")
		case math.IsInf(x, 0):
			return 0, errors.New("float is Inf")
		case math.Abs(x-float64(int64(x))) > 1e-9:
			return 0, errors.New("float to int64")
		}
		return int64(x), nil
	case string:
		s = x
	case []byte:
		s = string(x)
	default:
		s = fmt.Sprint(x)
	}
	v, err := strconv.ParseInt(s, 10, 64)
	return v, errors.Trace(err)
}
