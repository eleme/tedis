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
	"bytes"

	"encoding/binary"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/codec"
)

// TypeFlag is for data structure meta/data flag.
type TypeFlag byte

const (
	MetaCode TypeFlag = '#'
	DataCode TypeFlag = '&'
	// for zset
	indexCode TypeFlag = '+'
	// StringMeta is the flag for string meta.
	//StringMeta TypeFlag = 'S'
	// StringData is the flag for string data.
	StringData TypeFlag = 's'
	// HashMeta is the flag for hash meta.
	//HashMeta TypeFlag = 'H'
	// HashData is the flag for hash data.
	HashData TypeFlag = 'h'
	// ListMeta is the flag for list meta.
	ListMeta TypeFlag = 'L'
	// ListData is the flag for list data.
	ListData TypeFlag = 'l'
	ERRCode  TypeFlag = 'E'
)

type MetaValue struct {
	flag   TypeFlag
	expire int64
}

func EncodeStringMetaValue(expire int64) []byte {
	buf := make([]byte, 9)
	buf[0] = byte(StringData)
	binary.BigEndian.PutUint64(buf[1:], uint64(expire))
	return buf
}

func EncodeHashMetaValue(expireAt int64, count int64) []byte {
	buf := make([]byte, 17)
	buf[0] = byte(HashData)
	binary.BigEndian.PutUint64(buf[1:9], uint64(expireAt))
	binary.BigEndian.PutUint64(buf[9:], uint64(count))
	return buf
}

func DecodeMetaValue(value []byte) (TypeFlag, int64, int64) {
	flag := TypeFlag(value[0])
	expire := int64(binary.BigEndian.Uint64(value[1:9]))
	var len int64
	if flag == HashData {
		len = int64(binary.BigEndian.Uint64(value[9:]))
	}

	return flag, expire, len
}

func EncodeDataKey(dataFlag TypeFlag, prefix []byte, key []byte) kv.Key {
	ek := make([]byte, 0, len(prefix)+len(key)+24)
	ek = append(ek, prefix...)
	ek = codec.EncodeBytes(ek, key)
	return codec.EncodeUint(ek, uint64(dataFlag))
}

func (t *TxStructure) encodeStringDataKey(key []byte) kv.Key {
	// for codec Encode, we may add extra bytes data, so here and following encode
	// we will use extra length like 4 for a little optimization.
	ek := make([]byte, 0, len(t.prefix)+len(key)+24)
	ek = append(ek, t.prefix...)
	ek = codec.EncodeBytes(ek, key)
	return codec.EncodeUint(ek, uint64(DataCode))
}

func (t *TxStructure) EncodeMetaKey(key []byte) kv.Key {
	// for codec Encode, we may add extra bytes data, so here and following encode
	// we will use extra length like 4 for a little optimization.
	ek := make([]byte, 0, len(t.prefix)+len(key)+24)
	ek = append(ek, t.prefix...)
	ek = codec.EncodeBytes(ek, key)
	return codec.EncodeUint(ek, uint64(MetaCode))
}

func DecodeMetaKey(prefix []byte, ek kv.Key) (TypeFlag, []byte, error) {
	var (
		key []byte
		err error
		tp  uint64
	)

	if !bytes.HasPrefix(ek, prefix) {
		return ERRCode, nil, errors.New("invalid encoded hash data key prefix")
	}

	ek = ek[len(prefix):]

	ek, key, err = codec.DecodeBytes(ek)
	if err != nil {
		return ERRCode, nil, errors.Trace(err)
	}

	ek, tp, err = codec.DecodeUint(ek)
	if err != nil {
		return ERRCode, nil, errors.Trace(err)
	}

	return TypeFlag(tp), key, errors.Trace(err)
}

func (t *TxStructure) encodeMetaValue(key []byte) kv.Key {
	// for codec Encode, we may add extra bytes data, so here and following encode
	// we will use extra length like 4 for a little optimization.
	ek := make([]byte, 0, len(t.prefix)+len(key)+24)
	ek = append(ek, t.prefix...)
	ek = codec.EncodeBytes(ek, key)
	return codec.EncodeUint(ek, uint64(MetaCode))
}

func (t *TxStructure) encodeHashDataKey(key []byte, field []byte) kv.Key {
	ek := make([]byte, 0, len(t.prefix)+len(key)+len(field)+30)
	ek = append(ek, t.prefix...)
	ek = codec.EncodeBytes(ek, key)
	ek = codec.EncodeUint(ek, uint64(HashData))
	return codec.EncodeBytes(ek, field)
}

func (t *TxStructure) encodeMergedHashDataKey(key []byte) kv.Key {
	// for codec Encode, we may add extra bytes data, so here and following encode
	// we will use extra length like 4 for a little optimization.
	ek := make([]byte, 0, len(t.prefix)+len(key)+24)
	ek = append(ek, t.prefix...)
	ek = codec.EncodeBytes(ek, key)
	return codec.EncodeUint(ek, uint64(HashData))
}

func (t *TxStructure) decodeHashDataKey(ek kv.Key) ([]byte, []byte, error) {
	var (
		key   []byte
		field []byte
		err   error
		tp    uint64
	)

	if !bytes.HasPrefix(ek, t.prefix) {
		return nil, nil, errors.New("invalid encoded hash data key prefix")
	}

	ek = ek[len(t.prefix):]

	ek, key, err = codec.DecodeBytes(ek)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	ek, tp, err = codec.DecodeUint(ek)
	if err != nil {
		return nil, nil, errors.Trace(err)
	} else if TypeFlag(tp) != HashData {
		return nil, nil, errInvalidHashKeyFlag.Gen("invalid encoded hash data key flag %c", byte(tp))
	}

	_, field, err = codec.DecodeBytes(ek)
	return key, field, errors.Trace(err)
}

func (t *TxStructure) hashDataKeyPrefix(key []byte) kv.Key {
	ek := make([]byte, 0, len(t.prefix)+len(key)+24)
	ek = append(ek, t.prefix...)
	ek = codec.EncodeBytes(ek, key)
	return codec.EncodeUint(ek, uint64(HashData))
}

func (t *TxStructure) encodeListMetaKey(key []byte) kv.Key {
	ek := make([]byte, 0, len(t.prefix)+len(key)+24)
	ek = append(ek, t.prefix...)
	ek = codec.EncodeBytes(ek, key)
	return codec.EncodeUint(ek, uint64(ListMeta))
}

func (t *TxStructure) encodeListDataKey(key []byte, index int64) kv.Key {
	ek := make([]byte, 0, len(t.prefix)+len(key)+36)
	ek = append(ek, t.prefix...)
	ek = codec.EncodeBytes(ek, key)
	ek = codec.EncodeUint(ek, uint64(ListData))
	return codec.EncodeInt(ek, index)
}
