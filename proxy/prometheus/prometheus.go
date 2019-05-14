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

package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	CmdCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "proxy_cmd_total",
			Help: "Counter of proxy commands.",
		}, []string{"command"})

	PrintInfoCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "Print_info_count",
			Help: "Counter of infomation of code.",
		}, []string{"method"})

	TimeOutCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "TimeOut_cmd_count",
			Help: "Counter of timeout command",
		}, []string{"command"})

	CmdHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "proxy_cmd_duration_seconds",
			Help:    "Bucketed histogram of processing time of txn cmds.",
			Buckets: prometheus.LinearBuckets(5, 10, 100),
		}, []string{"command"})

	LengthHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "proxy_cmd_valuelength",
			Help:    "Bucketed histogram of value length",
			Buckets: prometheus.LinearBuckets(5, 20, 500),
		}, []string{"command"})

	TimeOutThresholds int
)

func init() {
	prometheus.MustRegister(PrintInfoCounter)
	prometheus.MustRegister(TimeOutCounter)
	prometheus.MustRegister(CmdCounter)
	prometheus.MustRegister(CmdHistogram)
	prometheus.MustRegister(LengthHistogram)
}

func PrintTimeOut(timedata int, method string) {
	if timedata > TimeOutThresholds {
		TimeOutCounter.WithLabelValues(method).Inc()
	}
}
