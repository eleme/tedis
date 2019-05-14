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

package util

import "time"

const (
	MaxExpireAt = 1e15
)

func Nowms() int64 {
	return int64(time.Now().UnixNano()) / int64(time.Millisecond)
}

func IsExpired(expireat int64) bool {
	return expireat != 0 && expireat <= Nowms()
}

func ExpireAtToTTLms(expireat int64) (int64, bool) {
	switch {
	case expireat > MaxExpireAt:
		return -1, false
	case expireat == 0:
		return -1, true
	default:
		if now := Nowms(); now < expireat {
			return int64(expireat - now), true
		} else {
			return 0, true
		}
	}
}

func TTLsToExpireAt(ttls int64) (int64, bool) {
	if ttls < 0 || ttls > MaxExpireAt/1e3 {
		return 0, false
	}
	return TTLmsToExpireAt(ttls * 1e3)
}

func TTLmsToExpireAt(ttlms int64) (int64, bool) {
	if ttlms < 0 || ttlms > MaxExpireAt {
		return 0, false
	}
	expireat := Nowms() + ttlms
	if expireat > MaxExpireAt {
		return 0, false
	}
	return expireat, true
}
