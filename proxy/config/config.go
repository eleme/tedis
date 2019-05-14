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

package config

import (
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"ekvproxy/proxy/log"
)

var ConfigData Conf;

type Conf struct {
	Port       int    `yaml:"port"`
	TurnOnAuth bool   `yaml:trun_on_auth`
	Passwd     string `yaml:"passwd"`
	Pdaddr     string `yaml:"pdaddr"`
	Logpath    string `yaml:"logfile"`
	Loglevel   string `yaml:"loglevel"`
	Ssaddr     string `yaml:"statsaddr"`
	Ssname     string `yaml:"stats"`
}

func ParseConf(path *string, config *Conf) () {
	yamlFile, err := ioutil.ReadFile(*path)
	if err != nil {
		log.Errorf("read config %s file error: %s", path, err)
	}
	err = yaml.Unmarshal(yamlFile, config)
	if err != nil {
		log.Errorf("parse config %s file error: %s", path, err)
	}
}
