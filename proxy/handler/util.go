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

package handler

import (
	"ekvproxy/proxy/uuid"
	"ekvproxy/proxy/log"
	"github.com/pingcap/tidb/kv"
	"github.com/juju/errors"
	"ekvproxy/proxy/prometheus"
)

const (
	MaxRetryCount int = 5
	//max key size
	MaxKeySize int = 1024 * 3
	//max value size
	MaxValueSize int = 1024 * 1024 * 5
)

func CheckKeySize(key []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return ErrKeySize
	}
	return nil
}

func CheckValueSize(value []byte) error {
	if len(value) > MaxValueSize {
		return ErrValueSize
	}

	return nil
}

type RequestContext struct {
	id  uuid.UUID
	cmd string
}

func newRequestContext(commond string) *RequestContext {
	return &RequestContext{
		id:  uuid.NewV4(),
		cmd: commond,
	}
}

func CallWithRetry(context *RequestContext, fn func() (interface{}, error)) (interface{}, error) {
	curCount := 0
	for {
		curCount++
		res, err := fn()
		if err == nil {
			return res, err
		}
		log.Errorf("%s retry:%d error:%s", context.id, curCount, errors.ErrorStack(err))
		if curCount >= MaxRetryCount {
			log.Errorf("%s Retry reached max count %d error: %s", context.id, curCount, err)
			return res, err
		}

		if !kv.IsRetryableError(err) {
			prometheus.CmdCounter.WithLabelValues("error" + context.cmd).Inc()
			return res, err
		}
	}

}
