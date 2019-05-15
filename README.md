# EKVProxy 
EKVProxy 是一个支持redis 协议的Tikv 的代理层。

## 简介
* EKVProxy 针对Tikv做的一个代理层，其兼容redis 的部分数据结构string、hash的大部分接口；
* EKVProxy 的组成包括支持事务版本的proxy，支持非事务版本的rawproxy，用于调用Tikv 的gc和用于删除过期数据的ttltask。
* EKVProxy 的工作机理是通过Ti的pd 获取路由信息，然后通过hash的方式将具体的请求发送到指定的server上，通过这种方式讲不同的请求发送到不同的节点上。

## 特点
* 支持redis协议。
* 支持多节点分布式部署，从而减轻代理层的压力和提高代理层的安全性。
* 支持事务和非事务两种版本。
* 采用go语言编写。

## 引用说明
### 直接引用
* 该项目中直接引用的项目有go-redis-server。
* 还有用到的有部分TiDB的代码和包。

## 使用
### 编译方式
编译：make,	清除：make clean 

### 参数说明
#### proxy 参数说明
```
-v		proxy 版本信息
-port		proxy 提供服务的端口
-conf   	proxy配置文件的文件名
-pd		TiDB的 pd 节点的地址 如：127.0.0.1:2379
-lp		日志保存目录
-ll		proxy 日志打印级别 如："info"
-kpdays 	日志保留时间 如："7"， 保留7天
-it		是否忽略ttl信息，因为TiKV是采用ttltask 对数据进行清除的，数据的清除不具有实时性，可能read的key已经过期
-ct		链接保持的超时时间
-td		请求超时时间
-pp		prometheus 服务端口
```
#### gc 参数说明
```
-v		gc 的版本信息
-pd		pd的地址信息，gc 的通过pd调用TiDB的gc
```
#### ttltask 参数说明
```
-v		ttltask 版本信息
-fs		当耗时超过改时间时，将停止运行
-pd    		TiDB的 pd 节点的地址 如：127.0.0.1:2379
-lp     	日志保存目录
-ll     	proxy 日志打印级别 如："info"
-cn		并发数量
-td		请求超时时间
-pp		prometheus 服务端口
```
### 运行
#### rawkvproxy 运行方式
```
nohup rawkvproxy" \
        -conf "./conf/rawkvproxy.conf" \
        -ct "180" \
        -kpdays "7" \
        -ll "error" \
        -lp "./log/rawkvproxy.log" \
        -pd "127.0.0.1:10001,127.0.0.1:10001,127.0.0.1:10001" \
        -port "6390" \
        -td "1500" \
	-ct 3600 \
	2> "./log/ttltask_stderr.log"
```
#### gc 运行方式
```
nohup bin/gc" -pd "127.0.0.1:10001,127.0.0.1:10001,127.0.0.1:10001" 2> "./log/ttltask_stderr.log"
```
#### ttltask 运行方式
```
nohup ttltask" \
        -cn "5" \
	-fs "1000" \
        -ll "error" \
        -lp "./log/ttltask.log" \
        -pd "127.0.0.1:10001,127.0.0.1:10001,127.0.0.1:10001" \
    	2> "./log/ttltask_stderr.log"
```
