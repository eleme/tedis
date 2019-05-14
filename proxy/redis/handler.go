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
	"reflect"
	"strings"
	"time"
	"ekvproxy/proxy/session"
	"ekvproxy/proxy/config"
	"ekvproxy/proxy/log"
	"ekvproxy/proxy/prometheus"
)

type HandlerFn func(r *Request) (ReplyWriter, error)

func (srv *Server) RegisterFct(key string, f interface{}) error {
	v := reflect.ValueOf(f)
	handlerFn, err := srv.createHandlerFn(f, &v)
	if err != nil {
		return err
	}
	srv.Register(key, handlerFn)
	return nil
}

func (srv *Server) Register(name string, fn HandlerFn) {
	if srv.methods == nil {
		srv.methods = make(map[string]HandlerFn)
	}
	if fn != nil {
		Debugf("REGISTER: %s", strings.ToLower(name))
		srv.methods[strings.ToLower(name)] = fn
	}
}

func (srv *Server) Apply(r *Request, session *session.Session) (reply ReplyWriter, err error) {
	if srv == nil || srv.methods == nil {
		log.Error("The method map is uninitialized")
		return ErrMethodNotSupported, nil
	}
	method := strings.ToLower(r.Name)
	start := time.Now().UnixNano() / 1e6
	defer func() {
		end := time.Now().UnixNano() / 1e6
		prometheus.CmdHistogram.WithLabelValues(method).Observe(float64(end - start))
		prometheus.PrintTimeOut(int(time.Now().UnixNano()/1e6-start), method)

		_, ok := reply.(*ErrorReply)
		if ok {
			prometheus.PrintInfoCounter.WithLabelValues("error_" + method).Inc()
		}
	}()

	log.Info(r)
	if method == "set" && len(r.Args) == 3 {
		method = "setwithttl";
		//log.Info(method)
	} else if method == "set" && len(r.Args) == 5 {
		method = "setwithttl"
		r.Args = append(r.Args[:2], r.Args[4])
	}

	fn, exists := srv.methods[method]
	if !exists {
		log.Error("method is not supported:", method)
		return ErrMethodNotSupported, nil
	}
	var packLen = 0
	for _, arg := range r.Args {
		packLen += len(arg)
	}
	if packLen > 5*1024*1024 {
		return ErrArgsTooLong, nil
	}
	if config.ConfigData.TurnOnAuth {
		if !session.Authed && method == "auth" {
			return fn(r)
		} else if !session.Authed {
			fn, exists = srv.methods["noauth"]
			if !exists {
				log.Error("method is not supported:", method)
				return ErrMethodNotSupported, nil
			}
			return fn(r)
		}
	}

	return fn(r)
}

//func (srv *Server) ApplyString(r *Request) (string, error) {
//	reply, err := srv.Apply(r)
//	if err != nil {
//		return "", err
//	}
//	return ReplyToString(reply)
//}
