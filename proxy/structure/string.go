// Copyright 2015 PingCAP, Inc.
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

package structure

import (
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/terror"
	"ekvproxy/proxy/util"
	"ekvproxy/proxy/prometheus"
)

// Set sets the string value of the key.
func (t *TxStructure) Set(key []byte, value []byte) ([]byte, error) {
	if t.readWriter == nil {
		return nil, errWriteOnSnapshot
	}
	prometheus.LengthHistogram.WithLabelValues("set").Observe(float64(len(value)))
	mk := t.EncodeMetaKey(key)
	mv, err := t.reader.Get(mk)
	if terror.ErrorEqual(err, kv.ErrNotExist) {
		err = nil
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if mv != nil {
		flag, _, _ := DecodeMetaValue(mv)
		if flag != StringData {
			return nil, errors.Trace(ErrSetType)
		}
	}
	ek := t.encodeStringDataKey(key)
	verr := t.readWriter.Set(ek, value)
	if verr != nil {
		return nil, errors.Trace(verr)
	}
	merr := t.readWriter.Set(mk, EncodeStringMetaValue(0))
	if merr != nil {
		return nil, errors.Trace(merr)
	}
	return []byte("OK"), nil
}

func (t *TxStructure) SetWithTTL(key []byte, value []byte, ttl int64) ([]byte, error) {
	if t.readWriter == nil {
		return nil, errWriteOnSnapshot
	}
	prometheus.LengthHistogram.WithLabelValues("setwithttl").Observe(float64(len(value)))
	mk := t.EncodeMetaKey(key)
	mv, err := t.reader.Get(mk)
	if terror.ErrorEqual(err, kv.ErrNotExist) {
		err = nil
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if mv != nil {
		flag, _, _ := DecodeMetaValue(mv)
		if flag != StringData {
			return nil, errors.Trace(ErrSetType)
		}
	}
	ek := t.encodeStringDataKey(key)
	verr := t.readWriter.Set(ek, value)
	if verr != nil {
		return nil, errors.Trace(verr)
	}

	var expireAt int64;
	if v, ok := util.TTLsToExpireAt(ttl); ok && v > 0 {
		expireAt = v
	} else {
		return nil, ErrInvalidTTL
	}

	merr := t.readWriter.Set(mk, EncodeStringMetaValue(expireAt))
	if merr != nil {
		return nil, errors.Trace(merr)
	}
	return []byte("OK"), nil
}

// Get gets the string value of a key.
func (t *TxStructure) Get(key []byte) ([]byte, error) {

	if !t.ignoreTTL {
		expired, err := t.checkStringExpired(key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if expired {
			return nil, nil
		}
	}

	ek := t.encodeStringDataKey(key)
	value, err := t.reader.Get(ek)
	if terror.ErrorEqual(err, kv.ErrNotExist) {
		err = nil
	}
	return value, errors.Trace(err)
}

// GetInt64 gets the int64 value of a key.
func (t *TxStructure) GetInt64(key []byte) (int64, error) {
	v, err := t.Get(key)
	if err != nil || v == nil {
		return 0, errors.Trace(err)
	}

	n, err := strconv.ParseInt(string(v), 10, 64)
	return n, errors.Trace(err)
}

func (t *TxStructure) IncInt64(key []byte, step int64) (int64, error) {
	val, err := t.Get(key)
	if (err == nil && (val == nil || len(val) == 0)) {
		_, err = t.Set(key, []byte(strconv.FormatInt(step, 10)))
		if err != nil {
			return 0, errors.Trace(err)
		}
		return step, nil
	}
	if err != nil {
		return 0, errors.Trace(err)
	}

	intVal, err := strconv.ParseInt(string(val), 10, 0)
	if err != nil {
		return 0, errors.Trace(ErrIncrValue)
	}

	intVal += step
	_, err = t.Set(key, []byte(strconv.FormatInt(intVal, 10)))
	if err != nil {
		return 0, errors.Trace(err)
	}
	return intVal, nil
}

//// Inc increments the integer value of a key by step, returns
//// the value after the increment.
//func (t *TxStructure) Inc(key []byte, step int64) (int64, error) {
//	if t.readWriter == nil {
//		return 0, errWriteOnSnapshot
//	}
//	//ek := t.encodeStringDataKey(key)
//	// txn Inc will lock this key, so we don't lock it here.
//	n, err := t.IncInt64(key, step)
//	if terror.ErrorEqual(err, kv.ErrNotExist) {
//		err = nil
//	}
//	return n, errors.Trace(err)
//}

// Clear removes the string value of the key.
func (t *TxStructure) Clear(key []byte) error {
	if t.readWriter == nil {
		return errWriteOnSnapshot
	}
	mk := t.encodeMetaValue(key)
	err := t.readWriter.Delete(mk)
	if terror.ErrorEqual(err, kv.ErrNotExist) {
		err = nil
	}
	if err != nil {
		return errors.Trace(err)
	}
	ek := t.encodeStringDataKey(key)
	err = t.readWriter.Delete(ek)
	if terror.ErrorEqual(err, kv.ErrNotExist) {
		err = nil
	}
	return errors.Trace(err)
}

func (t *TxStructure) checkStringExpired(key []byte) (bool, error) {
	metaKey := t.EncodeMetaKey(key)
	expireAt, exist, err := t.loadStringMeta(metaKey)
	if err != nil {
		return false, errors.Trace(err)
	}

	if !exist {
		return true, nil
	}

	return util.IsExpired(expireAt), nil
}

func (t *TxStructure) loadStringMeta(metaKey []byte) (int64, bool, error) {
	v, err := t.reader.Get(metaKey)
	if terror.ErrorEqual(err, kv.ErrNotExist) || len(v) < 1 {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, errors.Trace(err)
	}

	flag, expireAt, _ := DecodeMetaValue(v)
	if flag != StringData {
		return 0, true, errInvalidHashKeyFlag
	}
	return expireAt, true, nil
}
