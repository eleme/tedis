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

package structure

import (
	"ekvproxy/proxy/util"
	"github.com/juju/errors"
	//"github.com/pingcap/tidb/terror"
	//"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/kv"
)

var (
	InvalidFlag = errors.New("invalid flag")
)

func (t *TxStructure) DEL(keys [][]byte) (int, error) {
	if t.readWriter == nil {
		return 0, errWriteOnSnapshot
	}
	ms := &util.MarkSet{}

	var intererr error
	for _, key := range keys {
		if !ms.Has(key) {
			mk := t.EncodeMetaKey(key)
			mv, _ := t.reader.Get(mk)
			if mv != nil {
				flag, _, _ := DecodeMetaValue(mv)
				switch flag {
				case StringData:
					err := t.Clear(key)
					if err != nil {
						return 0, err
					}
					ms.Set(key)
				case HashData:
					//err := t.HClear(key)
					err := t.MergedHClear(key)
					if err != nil {
						return 0, err
					}
					ms.Set(key)
				default:
					return 0, InvalidFlag
				}

			}

		}
	}

	return int(ms.Len()), intererr

}

func (t *TxStructure) TTL(key []byte) (int, error) {
	if t.readWriter == nil {
		return 0, errWriteOnSnapshot
	}

	mk := t.EncodeMetaKey(key)
	mv, err := t.reader.Get(mk)

	if terror.ErrorEqual(err, kv.ErrNotExist) || len(mv) < 1 {
		return -2, nil
	}
	if err != nil {
		return 0, err
	}
	_, expireAt, _ := DecodeMetaValue(mv)
	ttlms, _ := util.ExpireAtToTTLms(expireAt)
	if err != nil || ttlms < 0 {
		return int(ttlms), errors.Trace(err)
	}
	ttls := ttlms / 1e3
	if int(ttls) == 0 {
		ttls = -2
	}
	return int(ttls), nil
}

func (t *TxStructure) EXPIRE(key []byte, ttl []byte) (int, error) {
	if t.readWriter == nil {
		return 0, errWriteOnSnapshot
	}

	ttls, err := util.ParseInt(ttl)
	if err != nil {
		return 0, errors.Trace(err)
	}

	mk := t.EncodeMetaKey(key)
	mv, err := t.reader.Get(mk)
	if terror.ErrorEqual(err, kv.ErrNotExist) || len(mv) < 1 {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	flag, expireAt, FieldCount := DecodeMetaValue(mv)
	if util.IsExpired(expireAt) {
		return 0, err
	}

	if v, ok := util.TTLsToExpireAt(ttls); ok && v > 0 {
		expireAt = v
	} else {
		return 0, ErrInvalidTTL
	}
	switch flag {
	case StringData:
		mv = EncodeStringMetaValue(expireAt)
	case HashData:
		mv = EncodeHashMetaValue(expireAt, FieldCount)
	default:
		return 0, InvalidFlag
	}

	t.readWriter.Set(mk, mv)

	return 1, nil

}

func (t *TxStructure) ExpireAt(key []byte, ttl []byte) (int, error) {
	if t.readWriter == nil {
		return 0, errWriteOnSnapshot
	}

	timestamp, err := util.ParseInt(ttl)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if timestamp > util.MaxExpireAt/1e3 {
		return 0, ErrInvalidTTL
	}

	newExpireat := int64(1)
	if timestamp != 0 {
		newExpireat = timestamp * 1e3
	}

	mk := t.EncodeMetaKey(key)
	mv, err := t.reader.Get(mk)
	if terror.ErrorEqual(err, kv.ErrNotExist) || len(mv) < 1 {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	flag, expireAt, FieldCount := DecodeMetaValue(mv)
	if util.IsExpired(expireAt) {
		return 0, err
	}

	switch flag {
	case StringData:
		mv = EncodeStringMetaValue(newExpireat)
	case HashData:
		mv = EncodeHashMetaValue(newExpireat, FieldCount)
	default:
		return 0, InvalidFlag
	}

	t.readWriter.Set(mk, mv)

	return 1, nil

}

func (t *TxStructure) loadMeta(metaKey []byte) (int64, bool, error) {
	v, err := t.reader.Get(metaKey)
	if terror.ErrorEqual(err, kv.ErrNotExist) || len(v) < 1 {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, errors.Trace(err)
	}

	_, expireAt, _ := DecodeMetaValue(v)
	return expireAt, true, nil
}

func (t *TxStructure) CheckExpireAndDel(keys [][]byte) (bool, error) {
	if t.readWriter == nil {
		return false, errWriteOnSnapshot
	}
	ms := &util.MarkSet{}

	var intererr error
	for _, key := range keys {
		if !ms.Has(key) {
			mk := t.EncodeMetaKey(key)
			mv, _ := t.reader.Get(mk)
			if mv != nil {
				flag, expireAt, _ := DecodeMetaValue(mv)
				if util.IsExpired(expireAt) {
					switch flag {
					case StringData:
						err := t.Clear(key)
						if err != nil {
							return false, err
						}
						ms.Set(key)
					case HashData:
						//err := t.HClear(key)
						err := t.MergedHClear(key)
						if err != nil {
							return false, err
						}
						ms.Set(key)
					default:
						return false, InvalidFlag
					}
				}

			}

		}
	}

	return true, intererr
}
