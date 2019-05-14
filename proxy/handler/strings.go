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
	"ekvproxy/proxy/structure"
	"github.com/juju/errors"
	"ekvproxy/proxy/log"
	"ekvproxy/proxy/util"
	goctx "golang.org/x/net/context"
	"ekvproxy/proxy/prometheus"
)

func (h *TxTikvHandler) SET(key []byte, value []byte) ([]byte, error) {
	context := newRequestContext("set")
	log.Infof("%s set %s %s", context.id, key, value)

	if kerr := CheckKeySize(key); kerr != nil {
		log.Errorf("%s  key:%s", context.id, key, kerr)
		return nil, kerr
	}
	if len(value) < 1 {
		return nil, ErrValueNil
	}
	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return nil, errors.Trace(ErrBegionTXN)
		}
		tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
		res, ierr := tx.Set(key, value)
		if ierr == nil {
			ierr = txn.Commit(goctx.Background())
		}

		if ierr != nil {
			txn.Rollback()
		}
		return res, ierr
	})

	return res.([]byte), err

}

func (h *TxTikvHandler) SETWITHTTL(key []byte, value []byte, ttl []byte) ([]byte, error) {
	context := newRequestContext("set with ttl")
	log.Infof("%s set %s %s %s", context.id, key, value, ttl)

	if kerr := CheckKeySize(key); kerr != nil {
		log.Errorf("%s  key:%s", context.id, key, kerr)
		return nil, kerr
	}
	if len(value) < 1 {
		return nil, ErrValueNil
	}

	ttls, err := util.ParseInt(ttl)
	if err != nil {
		return nil, errors.Trace(err)
	}

	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return nil, errors.Trace(ErrBegionTXN)
		}
		tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
		res, ierr := tx.SetWithTTL(key, value, ttls)
		if ierr == nil {
			ierr = txn.Commit(goctx.Background())
		}

		if ierr != nil {
			txn.Rollback()
		}
		return res, ierr
	})

	return res.([]byte), err

}

func (h *TxTikvHandler) GET(key []byte) ([]byte, error) {
	context := newRequestContext("set")
	log.Infof("%s get %s", context.id, key)

	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return nil, errors.Trace(ErrBegionTXN)
		}
		tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
		res, ierr := tx.Get(key)
		if ierr == nil {
			ierr = txn.Commit(goctx.Background())
		}

		if ierr != nil {
			txn.Rollback()
			log.Error(ierr)
		}
		return res, ierr
	})

	ba := res.([]byte)
	if err == nil && len(ba) > 0 {
		prometheus.CmdCounter.WithLabelValues("gethit").Inc()
	}
	return ba, err
}

func (h *TxTikvHandler) MSET(args [][]byte) ([]byte, error) {
	context := newRequestContext("mset")
	log.Infof("%s mset %s", context.id, args)
	if len(args) == 0 || len(args)%2 != 0 {
		return nil, errArguments("len(args) = %d, expect != 0 && mod 2 = 0", len(args))
	}

	for i := len(args)/2 - 1; i >= 0; i-- {
		key, _ := args[i*2], args[i*2+1]
		if kerr := CheckKeySize(key); kerr != nil {
			log.Errorf("%s  key:%s", context.id, key, kerr)
			return nil, kerr
		}
	}

	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return nil, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
		var ierr error
		for i := len(args)/2 - 1; i >= 0; i-- {
			key, value := args[i*2], args[i*2+1]

			_, err := tx.Set(key, value)
			if err != nil {
				ierr = err
				break
			}

		}
		if ierr == nil {
			ierr = txn.Commit(goctx.Background())
		}

		if ierr != nil {
			txn.Rollback()
		}
		return []byte("OK"), ierr
	})

	return res.([]byte), err
}

func (h *TxTikvHandler) MGET(args [][]byte) ([][]byte, error) {
	if len(args) == 0 {
		return nil, errArguments("len(args) = %d, expect != 0", len(args))
	}

	keys := args

	//for _, key := range keys {
	//	if kerr := CheckKeySize(key); kerr != nil {
	//		return nil, kerr
	//	}
	//}

	values := make([][]byte, len(keys))

	context := newRequestContext("mget")
	log.Infof("%s mget %s", context.id, args)
	_, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return nil, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
		var ierr error
		for i, key := range keys {
			res, err := tx.Get(key)
			if err != nil {
				ierr = err
				break
			}
			values[i] = res
		}
		if ierr == nil {
			ierr = txn.Commit(goctx.Background())
		}

		if ierr != nil {
			txn.Rollback()
		}
		return nil, ierr
	})

	return values, err
}

func (h *TxTikvHandler) INCR(key []byte) (int64, error) {

	context := newRequestContext("incr")
	log.Infof("%s incr %s", context.id, key)
	if kerr := CheckKeySize(key); kerr != nil {
		log.Errorf("%s  key:%s", context.id, key, kerr)
		return 0, kerr
	}

	//res, err := CallWithRetry(context, func() (interface{}, error) {
	txn, err := h.Store.Begin()
	if err != nil {
		return 0, errors.Trace(ErrBegionTXN)
	}
	tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
	//res, ierr := tx.HSet(key, field, value)
	var step int64 = 1;
	res, ierr := tx.IncInt64(key, step)
	if ierr == nil {
		ierr = txn.Commit(goctx.Background())
		return res, ierr
	}
	if ierr != nil {
		txn.Rollback()
	}
	return res, ierr
	//})
}

func (h *TxTikvHandler) INCRBY(key []byte, step int) (int64, error) {

	context := newRequestContext("incrby")
	log.Infof("%s incrby %s", context.id, key)
	if kerr := CheckKeySize(key); kerr != nil {
		log.Errorf("%s  key:%s", context.id, key, kerr)
		return 0, kerr
	}

	//res, err := CallWithRetry(context, func() (interface{}, error) {
	txn, err := h.Store.Begin()
	if err != nil {
		return 0, errors.Trace(ErrBegionTXN)
	}
	tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
	//res, ierr := tx.HSet(key, field, value)
	res, ierr := tx.IncInt64(key, int64(step))
	if ierr == nil {
		ierr = txn.Commit(goctx.Background())
		return res, ierr
	}
	if ierr != nil {
		txn.Rollback()
	}
	return res, ierr
	//})
}
