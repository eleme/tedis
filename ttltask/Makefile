VERSION := ${shell cat ../version}
BUILD_TIME := ${shell date +%FT%T%z}
GIT_HASH := ${shell git rev-parse HEAD}

LDFLAGS=-ldflags "-X main.Version=${VERSION} -X main.GitHash=${GIT_HASH} -X main.BuildTime=${BUILD_TIME}"


all:
	GOOS=linux GOARCH=amd64 go build ${LDFLAGS} 

.PNONY: clean
clean:
	-rm ttltask
