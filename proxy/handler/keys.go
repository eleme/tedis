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
)

func (h *TxTikvHandler) DEL(keys [][]byte) (int, error) {
	if len(keys) == 0 {
		return 0, errArguments("len(args) = %d, expect != 0", len(keys))
	}

	context := newRequestContext("del")
	log.Infof("%s del %s", context.id, keys)
	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return 0, errors.Trace(ErrBegionTXN)
		}
		tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
		res, ierr := tx.DEL(keys)
		if ierr == nil {
			ierr = txn.Commit(goctx.Background())
		}

		if ierr != nil {
			txn.Rollback()
			log.Error(ierr)
		}
		return res, ierr
	})
	return res.(int), err

}

func (h *TxTikvHandler) EXPIRE(key []byte, btime []byte) (int, error) {
	if len(key) == 0 {
		return 0, errArguments("len(args) = %d, expect != 0", len(key))
	}

	context := newRequestContext("expire")
	log.Infof("%s expire %s %s", context.id, key, btime)
	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return 0, errors.Trace(ErrBegionTXN)
		}
		tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
		res, ierr := tx.EXPIRE(key, btime)
		if ierr == nil {
			ierr = txn.Commit(goctx.Background())
		}

		if ierr != nil {
			txn.Rollback()
			log.Error(ierr)
		}
		return res, ierr
	})
	return res.(int), err

}

func (h *TxTikvHandler) TTL(key []byte) (int, error) {
	if len(key) == 0 {
		return 0, errArguments("len(args) = %d, expect != 0", len(key))
	}

	context := newRequestContext("ttl")
	log.Infof("%s ttl %s", context.id, key)
	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return 0, errors.Trace(ErrBegionTXN)
		}
		tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
		res, ierr := tx.TTL(key)
		if ierr == nil {
			ierr = txn.Commit(goctx.Background())
		}

		if ierr != nil {
			txn.Rollback()
			log.Error(ierr)
		}
		return res, ierr
	})
	return res.(int), err

}

func (h *TxTikvHandler) EXPIREAT(key []byte, btime []byte) (int, error) {
	if len(key) == 0 {
		return 0, errArguments("len(args) = %d, expect != 0", len(key))
	}

	context := newRequestContext("expireat")
	log.Infof("%s expireat %s %s", context.id, key, btime)
	res, err := CallWithRetry(context, func() (interface{}, error) {
		txn, err := h.Store.Begin()
		if err != nil {
			return 0, errors.Trace(ErrBegionTXN)
		}

		tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
		res, ierr := tx.ExpireAt(key, btime)
		if ierr == nil {
			ierr = txn.Commit(goctx.Background())
		}

		if ierr != nil {
			txn.Rollback()
			log.Error(ierr)
		}
		return res, ierr
	})
	return res.(int), err

}

func (h *TxTikvHandler) CheckExpireAndDel(keys [][]byte) (bool, error) {
	txn, err := h.Store.Begin()
	if err != nil {
		return false, errors.Trace(ErrBegionTXN)
	}

	tx := structure.NewStructure(txn, txn, h.NameSpace, h.IgnoreTTL)
	_, err = tx.CheckExpireAndDel(keys)
	if err != nil {
		txn.Rollback()
		return false, err
	}
	err = txn.Commit(goctx.Background())
	return true, nil
}

type TxTikvInterface interface {
	DEL(keys [][]byte) (int, error)
}
