BINARY_NAME=rb-spooler
BUILD_DIR=bin

.PHONY: all build test rpm srpm clean

all: build

build:
	@mkdir -p $(BUILD_DIR)
	go build -ldflags "-s -w" -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/rb-spooler

test:
	go test -v -race ./...

rpm:
	$(MAKE) -C packaging/rpm rpm

srpm:
	$(MAKE) -C packaging/rpm srpm

clean:
	rm -rf $(BUILD_DIR)
	$(MAKE) -C packaging/rpm clean
