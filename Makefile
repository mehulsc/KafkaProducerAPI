.DEFAULT_GOAL := help
.PHONY : check lint install-linters dep test build format 

VERSION := $(shell git describe --always)

RFC_3339 := "+%Y-%m-%dT%H:%M:%SZ"
DATE := $(shell date -u $(RFC_3339))
COMMIT := $(shell git rev-parse HEAD)
BRANCH := $(shell git rev-parse --abbrev-ref HEAD)

OPTS?=GO111MODULE=on GOBIN=${PWD}/bin
BIN_DIR?=./bin

TEST_OPTS:=-tags no_ci -cover -timeout=5m

RACE_FLAG:=-race
GOARCH:=$(shell go env GOARCH)

ifneq (,$(findstring 64,$(GOARCH)))
    TEST_OPTS:=$(TEST_OPTS) $(RACE_FLAG)
endif

BUILDINFO_VERSION := -X main.version=$(VERSION)
BUILDINFO_DATE := -X main.date=$(DATE)
BUILDINFO_COMMIT := -X main.commit=$(COMMIT)
BUILDINFO_BRANCH := -X main.Branch=${BRANCH}

BUILDINFO?=-ldflags="$(BUILDINFO_VERSION) $(BUILDINFO_DATE) $(BUILDINFO_COMMIT)"

BUILD_OPTS?=$(BUILDINFO)

check: lint test ## Run linters and tests

lint: ## Run linters. Use make install-linters first	
	${OPTS} golangci-lint run -c .golangci.yml ./...
	# The govet version in golangci-lint is out of date and has spurious warnings, run it separately
	${OPTS} go vet -all ./...

vendorcheck:  ## Run vendorcheck
	GO111MODULE=off vendorcheck ./...

test: ## Run tests
	-go clean -testcache &>/dev/null
	${OPTS} go test ${TEST_OPTS} ./...

install-linters: ## Install linters
	- VERSION=1.23.8 ./ci_scripts/install-golangci-lint.sh
	# GO111MODULE=off go get -u github.com/FiloSottile/vendorcheck
	# For some reason this install method is not recommended, see https://github.com/golangci/golangci-lint#install
	# However, they suggest `curl ... | bash` which we should not do
	# ${OPTS} go get -u github.com/golangci/golangci-lint/cmd/golangci-lint
	${OPTS} go get -u golang.org/x/tools/cmd/goimports

format: ## Formats the code. Must have goimports installed (use make install-linters).
	${OPTS} goimports -w -local github.com/hello-error/KafkaProducerAPI .

dep: ## Sorts dependencies
	${OPTS} go mod download
	${OPTS} go mod tidy -v

build: ## Build binaries into ./bin
	${OPTS} go install ${BUILD_OPTS} ./cmd/*

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
