// go-redis-server is a helper library for building server software capable of speaking the redis protocol.
// This could be an alternate implementation of redis, a custom proxy to redis,
// or even a completely different backend capable of "masquerading" its API as a redis database.

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
	"fmt"
	"ekvproxy/proxy/log"
	"net"
	"reflect"
	"ekvproxy/proxy/session"
	"strings"
	"sync"
	"time"
	"github.com/juju/errors"
	"os"
	"runtime/debug"
	"ekvproxy/proxy/prometheus"
)

type Server struct {
	Proto        string
	Addr         string // TCP address to listen on, ":6389" if empty
	MonitorChans []chan string
	methods      map[string]HandlerFn

	l net.Listener

	// conn mutex
	mu sync.Mutex

	timeout int
	// conn map
	conns map[*Conn]struct{}
}

func (srv *Server) ListenAndServe() error {
	addr := srv.Addr
	if srv.Proto == "" {
		srv.Proto = "tcp"
	}
	if srv.Proto == "unix" && addr == "" {
		addr = "/tmp/redis.sock"
	} else if addr == "" {
		addr = ":6389"
	}
	log.Info(srv.Proto, addr)
	l, e := net.Listen(srv.Proto, addr)
	if e != nil {
		return e
	}
	srv.l = l
	return srv.Serve(l)
}

// Serve accepts incoming connections on the Listener l, creating a
// new service goroutine for each.  The service goroutines read requests and
// then call srv.Handler to reply to them.
func (srv *Server) Serve(l net.Listener) error {
	defer l.Close()
	srv.MonitorChans = []chan string{}

	for {
		rw, err := l.Accept()
		if err != nil {
			return err
		}
		go srv.ServeClient(rw)
	}
}

// Serve starts a new redis session, using `conn` as a transport.
// It reads commands using the redis protocol, passes them to `handler`,
// and returns the result.
func (srv *Server) ServeClient(conn net.Conn) (err error) {

	c := newConn(conn, srv.timeout)

	defer func() {
		if msg := recover(); msg != nil {
			log.Error(string(debug.Stack()))
			log.Error(c.summ, " panic:", msg)
			srv.removeConn(c)
			c.Close()
		}
	}()

	defer func() {
		if err != nil {
			log.Error(c.summ, err)
		}
		srv.removeConn(c)
		c.Close()
	}()
	srv.addConn(c)

	//clientChan := make(chan struct{})
	//// Read on `conn` in order to detect client disconnect
	//go func() {
	//	// Close chan in order to trigger eventual selects
	//	defer close(clientChan)
	//	defer Debugf("Client disconnected")
	//	// FIXME: move conn within the request.
	//	if false {
	//		io.Copy(ioutil.Discard,c.nc )
	//	}
	//}()

	var clientAddr string

	clientAddr = c.nc.RemoteAddr().String()
	var session session.Session
	//session.Conntime = time.Now()
	for {
		if c.timeout > 0 {
			deadline := time.Now().Add(c.timeout)
			if err := c.nc.SetReadDeadline(deadline); err != nil {
				return err
			}
		}
		request, err := parseRequest(c)
		if err != nil {
			return err
		}
		//session.Lasttime = time.Now()
		request.Host = clientAddr
		//request.ClientChan = clientChan
		reply, err := srv.Apply(request, &session)
		if err != nil {
			return err
		} else if strings.ToLower(request.Name) == "auth" {
			session.Authed = true
		}
		//if _, err = reply.WriteTo(c.w); err != nil {
		//	return err
		//}
		if c.timeout > 0 {
			deadline := time.Now().Add(c.timeout)
			if err := c.nc.SetWriteDeadline(deadline); err != nil {
				return errors.Trace(err)
			}
		}
		if err = c.writeRESP(reply); err != nil {
			return err
		}

	}
	return nil
}

func NewServer(c *Config, timeout int) (*Server, error) {
	srv := &Server{
		Proto:        c.proto,
		MonitorChans: []chan string{},
		methods:      make(map[string]HandlerFn),
		conns:        make(map[*Conn]struct{}),
		timeout:      timeout,
	}

	if srv.Proto == "unix" {
		srv.Addr = c.host
	} else {
		srv.Addr = fmt.Sprintf(":%d", c.port)
	}

	if c.handler == nil {
		c.handler = NewDefaultHandler()
	}

	log.Infof("TotalClient" + hostname())
	rh := reflect.TypeOf(c.handler)
	for i := 0; i < rh.NumMethod(); i++ {
		method := rh.Method(i)
		if method.Name[0] > 'a' && method.Name[0] < 'z' {
			continue
		}
		log.Info("Listening method:", method.Name)
		handlerFn, err := srv.createHandlerFn(c.handler, &method.Func)
		if err != nil {
			return nil, err
		}
		srv.Register(method.Name, handlerFn)
	}
	return srv, nil
}

func (srv *Server) addConn(c *Conn) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	prometheus.PrintInfoCounter.WithLabelValues("add_conn").Inc()
	srv.conns[c] = struct{}{}
}

func (srv *Server) removeConn(c *Conn) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	prometheus.PrintInfoCounter.WithLabelValues("del_conn").Inc()
	delete(srv.conns, c)
}

func (srv *Server) closeConns() {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	for c, _ := range srv.conns {
		c.Close()
	}

	srv.conns = make(map[*Conn]struct{})
}

func (srv *Server) close() {
	srv.l.Close()
	srv.closeConns()

}

func hostname() string {
	hname, _ := os.Hostname()
	hname = strings.Replace(hname, ".", "-", -1)
	if len(hname) == 0 {
		hname = "unknow-hostname"
	}

	return hname
}
