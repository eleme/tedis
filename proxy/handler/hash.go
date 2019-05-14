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
	goctx "golang.org/x/net/context"
	"ekvproxy/proxy/prometheus"
)

func (h *TxTikvHandler) HSET(key []byte, field []byte, value []byte) (int, error) {

	context := newRequestContext("hset")
	log.Infof("%s hset %s %s %s", context.id, key, field, value)
	if kerr := CheckKeySize(key); kerr != nil {
		log.Errorf("%s  key:%s", context.id, key, kerr)
		return 0, kerr
	}

	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return 0, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
		//res, ierr := tx.HSet(key, field, value)
		res, ierr := tx.MergedHSet(key, field, value)
		if ierr == nil {
			ierr = txn.Commit(goctx.Background())

		}

		if ierr != nil {
			txn.Rollback()
		}
		return res, ierr
	})

	return res.(int), errors.Trace(err)
}

func (h *TxTikvHandler) HMSET(args [][]byte) ([]byte, error) {
	key := args[0]

	context := newRequestContext("hmset")
	log.Infof("%s hmset %s %s", context.id, key, args)

	if len(args) == 1 || len(args)%2 != 1 {
		return nil, errArguments("len(args) = %d, expect != 1 && mod 2 = 1", len(args))
	}

	if kerr := CheckKeySize(key); kerr != nil {
		log.Errorf("%s  key:%s", context.id, key, kerr)

		return nil, kerr
	}

	var eles = make([]*structure.HashPair, len(args)/2)
	for i := 0; i < len(eles); i++ {
		e := &structure.HashPair{}
		e.Field = args[i*2+1]
		e.Value = args[i*2+2]
		eles[i] = e
	}

	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return 0, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
		//res, ierr := tx.HMSet(key, eles)
		res, ierr := tx.MergedHMSet(key, eles)
		if ierr == nil {
			ierr = txn.Commit(goctx.Background())
		}

		if ierr != nil {
			txn.Rollback()
		}
		return res, ierr
	})

	return res.([]byte), errors.Trace(err)
}

func (h *TxTikvHandler) HGET(key []byte, field []byte) ([]byte, error) {

	context := newRequestContext("hget")
	log.Infof("%s hget %s %s", context.id, key, field)
	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return nil, errors.Trace(ErrBegionTXN)
		}
		tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
		//res, ierr := tx.HGet(key, field)
		res, ierr := tx.MergedHGet(key, field)
		if ierr == nil {
			ierr = txn.Commit(goctx.Background())
		}
		if ierr != nil {
			txn.Rollback()
		}
		return res, ierr
	})
	ba := res.([]byte)

	if err == nil && len(ba) > 0 {
		prometheus.CmdCounter.WithLabelValues("hgethit").Inc()
	}
	return ba, err

}

func (h *TxTikvHandler) HGETALL(key []byte) ([][]byte, error) {
	context := newRequestContext("hgetall")
	log.Infof("%s hgetall %s", context.id, key)
	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return nil, errors.Trace(ErrBegionTXN)
		}
		tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
		//res, ierr := tx.HGetAll(key)
		res, ierr := tx.MergedHGetAll(key)
		if ierr == nil {
			ierr = txn.Commit(goctx.Background())
		}
		if ierr != nil {
			txn.Rollback()
		}

		return res, ierr
	})
	ba := res.([][]byte)
	if err == nil && len(ba) > 0 {
		prometheus.CmdCounter.WithLabelValues("hgetall").Inc()

	}
	return ba, err

}

func (h *TxTikvHandler) HDEL(key []byte, args [][]byte) (int, error) {
	if len(args) < 1 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	context := newRequestContext("hdel")
	log.Infof("%s hdel %s", context.id, args)
	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return 0, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
		//res, ierr := tx.HDel(key, args)
		res, ierr := tx.MergedHDel(key, args)
		if ierr == nil {
			ierr = txn.Commit(goctx.Background())
		}

		if ierr != nil {
			txn.Rollback()
		}
		return res, ierr
	})

	return res.(int), err
}

func (h *TxTikvHandler) HKEYS(key []byte) ([][]byte, error) {

	context := newRequestContext("hkeys")
	log.Infof("%s hkeys %s", context.id, key)
	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return nil, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
		//res, ierr := tx.HKeys(key)
		res, ierr := tx.MergedHKeys(key)
		if ierr == nil {
			ierr = txn.Commit(goctx.Background())
		}

		if ierr != nil {
			txn.Rollback()
		}
		return res, ierr
	})
	return res.([][]byte), err

}

func (h *TxTikvHandler) HLEN(key []byte) (int, error) {
	context := newRequestContext("hlen")
	log.Infof("%s hlen %s", context.id, key)
	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return nil, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
		res, ierr := tx.HLen(key)

		if ierr == nil {
			ierr = txn.Commit(goctx.Background())
		}

		if ierr != nil {
			txn.Rollback()
		}
		return res, ierr
	})
	return res.(int), err
}

func (h *TxTikvHandler) HMGET(key []byte, fields [][]byte) ([][]byte, error) {

	context := newRequestContext("hmget")
	log.Infof("%s hmget %s %s", context.id, key, fields)

	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return nil, errors.Trace(ErrBegionTXN)
		}
		tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
		res, ierr := tx.MergedHMGet(key, fields)

		if ierr == nil {
			ierr = txn.Commit(goctx.Background())
		}

		if ierr != nil {
			txn.Rollback()
		}
		return res, ierr
	})
	ba := res.([][]byte)

	if err == nil && len(ba) > 0 {
		prometheus.CmdCounter.WithLabelValues("hmget").Inc()
	}
	return ba, err

}

func (h *TxTikvHandler) HINCRBY(key []byte, field []byte, step int) (int64, error) {
	context := newRequestContext("hincrby")
	log.Infof("%s hincrby %s", context.id, key)
	if kerr := CheckKeySize(key); kerr != nil {
		log.Errorf("%s  key:%s", context.id, key, kerr)
		return 0, kerr
	}

	txn, err := h.Store.Begin()
	if err != nil {
		return 0, errors.Trace(ErrBegionTXN)
	}

	tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
	res, ierr := tx.MergedHIncInt64(key, field, int64(step))

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
