// Copyright (c) 2015 PingCAP, Inc.
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

package handler

import (
	"testing"

	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	"github.com/pingcap/tidb/util/testleak"
)

func TestTxTikvHandler(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testTxTikvHandlerSuite{})

type testTxTikvHandlerSuite struct {
	handler *TxTikvHandler
}

func (s *testTxTikvHandlerSuite) SetUpSuite(c *C) {
	path := "memory:"
	d := localstore.Driver{
		Driver: goleveldb.MemoryDriver{},
	}
	store, err := d.Open(path)
	c.Assert(err, IsNil)
	s.handler = &TxTikvHandler{store, []byte{0x00}}
}

func (s *testTxTikvHandlerSuite) TearDownSuite(c *C) {
	err := s.handler.Store.Close()
	c.Assert(err, IsNil)
}

func (s *testTxTikvHandlerSuite) TestString(c *C) {
	defer testleak.AfterTest(c)()

	key := []byte("a")
	value := []byte("1")
	_, err := s.handler.SET(key, value)
	c.Assert(err, IsNil)

	v, err := s.handler.GET(key)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, value)

	res, err := s.handler.DEL([][]byte{key})
	c.Assert(err, IsNil)
	c.Assert(res, Equals, 1)

	v, err = s.handler.GET(key)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	key1 := []byte("a")
	value1 := []byte("1")

	key2 := []byte("b")
	value2 := []byte("2")

	r, err := s.handler.MSET([][]byte{key1, value1, key2, value2})
	c.Assert(err, IsNil)
	c.Assert(r, DeepEquals, []byte("OK"))

	key3 := []byte("c")
	vs, err := s.handler.MGET([][]byte{key1, key2, key3})
	c.Assert(err, IsNil)
	c.Assert(3, Equals, len(vs))
	c.Assert(value1, DeepEquals, vs[0])
	c.Assert(value2, DeepEquals, vs[1])

	c.Assert(vs[2], IsNil)

}

type HashPair struct {
	Field []byte
	Value []byte
}

func (s *testTxTikvHandlerSuite) TestHash(c *C) {
	defer testleak.AfterTest(c)()

	key := []byte("hashtest1")
	field := []byte("1")
	value := []byte("1")
	r, err := s.handler.HSET(key, field, value)
	c.Assert(err, IsNil)
	c.Assert(r, Equals, 1)

	res, err := s.handler.HGET(key, field)
	c.Assert(err, IsNil)
	c.Assert(res, DeepEquals, value)

	r, err = s.handler.HSET(key, []byte("2"), []byte("2"))
	c.Assert(err, IsNil)
	c.Assert(r, Equals, 1)

	r, err = s.handler.HLEN(key)
	c.Assert(err, IsNil)
	c.Assert(r, Equals, 2)

	keys, err := s.handler.HKEYS(key)
	c.Assert(err, IsNil)
	c.Assert(len(keys), Equals, 2)
	c.Assert(keys, DeepEquals, [][]byte{[]byte("1"), []byte("2")})

	kvs, err := s.handler.HGETALL(key)
	c.Assert(err, IsNil)
	c.Assert(len(kvs), Equals, 4)
	c.Assert(kvs, DeepEquals, [][]byte{
		[]byte("1"), []byte("1"),
		[]byte("2"), []byte("2")})

	count, err := s.handler.HDEL(key, [][]byte{[]byte("1")})
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1)

	val, err := s.handler.HGET(key, field)
	c.Assert(err, IsNil)
	c.Assert(val, IsNil)

	r, err = s.handler.HLEN(key)
	c.Assert(err, IsNil)
	c.Assert(r, Equals, 1)

	sign, err := s.handler.HMSET([][]byte{key, []byte("3"), []byte("3"), []byte("4"), []byte("4")})
	c.Assert(err, IsNil)
	c.Assert(sign, DeepEquals, []byte("OK"))

	kvs, err = s.handler.HGETALL(key)
	c.Assert(err, IsNil)
	c.Assert(len(kvs), Equals, 6)
	c.Assert(kvs, DeepEquals, [][]byte{
		[]byte("2"), []byte("2"),
		[]byte("3"), []byte("3"),
		[]byte("4"), []byte("4")})

	count, err = s.handler.DEL([][]byte{key})
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1)

	val, err = s.handler.HGET(key, []byte("2"))
	c.Assert(err, IsNil)
	c.Assert(val, IsNil)
}
