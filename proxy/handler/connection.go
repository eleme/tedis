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

func (h *TxTikvHandler) QUIT() ([]byte, error) {
	return []byte("OK"), nil
}

func (h *TxTikvHandler) INFO() ([]byte, error) {
	return []byte("version:txnkv1.0.1"), nil
}

func (h *TxTikvHandler) PING() ([]byte, error) {
	return []byte("PONG"), nil
}
