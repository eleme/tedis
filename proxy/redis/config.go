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

type Config struct {
	proto   string
	host    string
	port    int
	handler interface{}
}

func DefaultConfig() *Config {
	return &Config{
		proto:   "tcp",
		host:    "127.0.0.1",
		port:    6389,
		handler: NewDefaultHandler(),
	}
}

func (c *Config) Port(p int) *Config {
	c.port = p
	return c
}

func (c *Config) Host(h string) *Config {
	c.host = h
	return c
}

func (c *Config) Proto(p string) *Config {
	c.proto = p
	return c
}

func (c *Config) Handler(h interface{}) *Config {
	c.handler = h
	return c
}
