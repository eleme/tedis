package main

import (
	"flag"
	"ekvproxy/proxy/log"
	"strings"
	"ekvproxy/proxy/redis"
)

var (
	serverPort = flag.Int("port", 6378, "listen port,default: 6379")
	logPath    = flag.String("lp", "", "log file path, if empty, default:stdout")
	logLevel   = flag.String("ll", "info", "log level:INFO|WARN|ERROR default:INFO")
)

type ConnTestHandler struct {
}

// Get override the DefaultHandler's method.
func (h *ConnTestHandler) GET(key []byte) ([]byte, error) {
	log.Info("get key:", key)
	return []byte("ok"), nil

}

func (h *ConnTestHandler) SET(key []byte, val []byte) error {
	log.Info("set key: ", key)
	return nil
}

func main() {
	flag.Parse()

	defer func() {
		if msg := recover(); msg != nil {
			log.Errorf("Panic: %v\n", msg)
		}
	}()

	initlog()

	myhandler := &ConnTestHandler{}
	srv, err := redis.NewServer(redis.DefaultConfig().Port(*serverPort).Handler(myhandler), 1000)
	if err != nil {
		panic(err)
	}

	if err := srv.ListenAndServe(); err != nil {
		panic(err)
	}
}

func initlog() {
	if len(*logPath) > 0 {
		log.SetHighlighting(false)
		err := log.SetOutputByName(*logPath)
		if err != nil {
			log.Fatalf("set log name failed - %s", err)
		}
	}

	switch strings.ToUpper(*logLevel) {
	case "INFO":
		log.SetLevel(log.LOG_LEVEL_INFO)
	case "ERROR":
		log.SetLevel(log.LOG_LEVEL_ERROR)
	case "WARN":
		log.SetLevel(log.LOG_LEVEL_WARN)
	default:
		log.SetLevel(log.LOG_LEVEL_INFO)
	}

	log.SetRotateByDay()
}
