ifndef VERBOSE
.SILENT:
endif

override CURRENT_DIR = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
override DOCKER_MOUNT_SUFFIX ?= consistent

ifeq ($(GO111MODULE),auto)
override GO111MODULE = on
endif

ifeq ($(OS),Windows_NT)
override ROOT_DIR = $(shell echo $(CURRENT_DIR) | sed -e "s:^/./:\U&:g")
else
override ROOT_DIR = $(CURRENT_DIR)
endif

generate: docker-protoc-generate go-inject-tag ## execute all generators & go-inject-tag
.PHONY: generate

docker-protoc-generate: init ## generate proto, grpc client & server
	. ${ROOT_DIR}/scripts/common.sh ${ROOT_DIR}/scripts ;\
	docker run --rm \
	 	-v /${ROOT_DIR}/api/proto:/${ROOT_DIR}/api/proto:${DOCKER_MOUNT_SUFFIX} \
	 	-v /$${PROTO_GEN_PATH}:/$${PROTO_GEN_PATH}:${DOCKER_MOUNT_SUFFIX} \
	 	-v /${ROOT_DIR}/configs/prototool.yaml:/${ROOT_DIR}/prototool.yaml:${DOCKER_MOUNT_SUFFIX} \
	 	-w /${ROOT_DIR} \
	 	$${PROTOTOOL_IMAGE}:$${PROTOTOOL_IMAGE_TAG} \
	 	prototool generate api/proto
.PHONY: docker-protoc-generate

go-inject-tag: ## inject tags into golang grpc structs
	. ${ROOT_DIR}/scripts/inject-tag.sh ${ROOT_DIR}/scripts
.PHONY: go-inject-tag

go-mockery: ## generate golang mock objects
	go get github.com/vektra/mockery/.../ ;\
	mockery -name=BillingService -recursive=true -output ${ROOT_DIR}/pkg/mocks
.PHONY: go-mockery

init:
	. ${ROOT_DIR}/scripts/common.sh ${ROOT_DIR}/scripts ;\
	mkdir -p $${PROTO_GEN_PATH}
.PHONY: init

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
.PHONY: help

.DEFAULT_GOAL := help