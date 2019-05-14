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
	"errors"
	"io"
)

var (
	ErrMethodNotSupported   = NewError("Method is not supported")
	ErrNotEnoughArgs        = NewError("Not enough arguments for the command")
	ErrTooMuchArgs          = NewError("Too many arguments for the command")
	ErrWrongArgsNumber      = NewError("Wrong number of arguments")
	ErrExpectInteger        = NewError("Expected integer")
	ErrExpectPositivInteger = NewError("Expected positive integer")
	ErrExpectMorePair       = NewError("Expected at least one key val pair")
	ErrExpectEvenPair       = NewError("Got uneven number of key val pairs")
	ErrArgsTooLong          = NewError("Args is too long")
)

var (
	ErrParseTimeout = errors.New("timeout is not an integer or out of range")
)

type ErrorReply struct {
	code    string
	message string
}

func (er *ErrorReply) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write([]byte("-" + er.code + " " + er.message + "\r\n"))
	return int64(n), err
}

func (er *ErrorReply) Error() string {
	return "-" + er.code + " " + er.message + "\r\n"
}

func NewError(message string) *ErrorReply {
	return &ErrorReply{code: "ERROR", message: message}
}
