#!/usr/bin/env sh
pwd
mkdir -p src/ekvproxy
mv gc src/ekvproxy
mv proxy src/ekvproxy
mv ttltask src/ekvproxy
mv vendor src/ekvproxy
mv conntest src/ekvproxy
export GOPATH=$GOPATH:`pwd`
echo $GOPATH
#yum -y update nss curl
#go get github.com/ngaut/log
#go get github.com/vmihailenco/msgpack
#go get gopkg.in/yaml.v2
#go get github.com/themester/GoSlaves
go build -o proxy src/ekvproxy/proxy/RedisProxyServer.go
go build -o rawkvproxy src/ekvproxy/proxy/rawkvproxy/ProxyServer.go
go build -o gc src/ekvproxy/gc/gc.go
#ls -l
#go build -o ttltask src/ekvproxy/ttltask/ttltask.go
