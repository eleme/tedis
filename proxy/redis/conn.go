// Copyright 2013 Docker, Inc.
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

package redis

import (
	"bufio"
	"sync"
	"net"
	"time"
	"fmt"
	"github.com/juju/errors"
	"ekvproxy/proxy/log"
)

type Conn struct {
	r *bufio.Reader
	w *bufio.Writer

	wLock sync.Mutex

	db uint32
	nc net.Conn

	// summary for this connection
	summ    string
	timeout time.Duration

	authenticated bool

	// whether sync from master or not
	isSyncing bool
}

func newConn(nc net.Conn, timeout int) *Conn {
	c := &Conn{
		nc: nc,
	}

	c.r = bufio.NewReader(nc)
	c.w = bufio.NewWriter(nc)
	c.summ = fmt.Sprintf("local%s-remote%s", nc.LocalAddr(), nc.RemoteAddr())
	c.timeout = time.Duration(timeout) * time.Second
	c.authenticated = false
	c.isSyncing = false
	log.Info("connection established:", c.summ)

	return c
}

func (c *Conn) Close() {
	c.nc.Close()
	c = nil
}

func (c *Conn) writeRESP(reply ReplyWriter) error {
	c.wLock.Lock()
	defer c.wLock.Unlock()
	reply.WriteTo(c.w)
	return errors.Trace(c.w.Flush())
}

func (c *Conn) flushRESP(reply ReplyWriter) error {
	c.wLock.Lock()
	defer c.wLock.Unlock()
	reply.WriteTo(c.w)
	c.w.Flush()
	return errors.Trace(c.w.Flush())
}
