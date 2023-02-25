
test:
	GOFLAGS="-count=1" go test -v ./...

build:
	cd cmd/server && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-s -w" -a -o ../../go-delayqueue . && docker-compose build

run:
	cd cmd/server && go run .
	
deploy:
	docker-compose push
	
local-down:
	docker stop go-delayqueue

local-up:
	docker-compose -f ./docker-compose-run-sample.yml up -d go-delayqueue

