build/platform: *.go
	mkdir -p build
	go build -o build/platform

build/container: dist/platform
	docker build -t platform .
	mkdir -p build
	touch build/container

dist/platform: *.go
	mkdir -p dist
	GOOS=linux GOARCH=amd64 go build -o dist/platform

.PHONY: docker
docker: build/container

.PHONY: release
release: build/container
	docker tag -f platform tutum.co/teltech/boots-platform
	docker push tutum.co/teltech/boots-platform
