
test:
	go test -v ./...

build:
	cd cmd/server && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-s -w" -a -o ../../go-delayqueue .

