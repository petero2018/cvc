
.PHONY: \
    image \
    login \
    push \
    build

.DEFAULT_GOAL:=help

SHELL = bash

#
# Variables
#

DOCKER_PUBLIC_REPOSITORY=

IMAGE_ARGS=PYTHON_VERSION=${PYTHON_VERSION} PYTHON_FLAVOUR=${PYTHON_FLAVOUR} POETRY_VERSION=${POETRY_VERSION} DOCKER_PUBLIC_REPOSITORY=${DOCKER_PUBLIC_REPOSITORY}
IMAGE_NAME_SHORT=cvc_mini_data_warehouse
IMAGE_NAME=${DOCKER_REPOSITORY}/${IMAGE_NAME_SHORT}
IMAGE_TAG=${PYTHON_VERSION}-${PR_ID}

# The version of the Docker image. Falls back gracefully to one of the values:
# - [tag] if HEAD is tagged
# - [tag]-[number of commits after the last tag]-g[short git commit hash] if HEAD has an offset from a tag
# - [short git commit hash]
VERSION=$(shell git -C . describe --tags 2> /dev/null || git -C . rev-parse --short HEAD)

# Optionally include variables from .env file.
-include .env

#
# Targets
#

PUSH_DEPLOY=false
PUSH_LATEST=false
PUSH_DEVEL=false

build: IMAGE_ARGS_PR=$(addprefix --build-arg ,$(IMAGE_ARGS))
build:
	@echo "Building Image $(IMAGE_NAME)"
	docker buildx build $(IMAGE_ARGS_PR) -t $(IMAGE_NAME)

push:

ifeq ($(PUSH_DEVEL),true)
	@echo "Pushing Image $(IMAGE_NAME):dev"
	@docker tag $(IMAGE_NAME) $(IMAGE_NAME):dev
	@docker push $(IMAGE_NAME):dev
endif
ifeq ($(PUSH_DEPLOY),true)
	@echo "Pushing Image $(IMAGE_NAME):$(IMAGE_TAG)"
	@docker tag $(IMAGE_NAME) $(IMAGE_NAME):$(IMAGE_TAG)
	@docker push $(IMAGE_NAME):$(IMAGE_TAG)
endif
ifeq ($(PUSH_LATEST),true)
	@echo "Pushing Image $(IMAGE_NAME):latest"
	@docker tag $(IMAGE_NAME) $(IMAGE_NAME):latest
	@docker push $(IMAGE_NAME):latest
endif

PULL_DEVEL=false
PULL_DEPLOY=false
PULL_LATEST=false

pull:

ifeq ($(PULL_DEVEL),true)
	@echo "Pulling Image $(IMAGE_NAME):dev"
	@docker pull $(IMAGE_NAME):dev
endif
ifeq ($(PULL_DEPLOY),true)
	@echo "Pulling Image $(IMAGE_NAME):$(IMAGE_TAG)"
	@docker pull $(IMAGE_NAME):$(IMAGE_TAG)
endif
ifeq ($(PULL_LATEST),true)
	@echo "Pulling Image $(IMAGE_NAME):latest"
	@docker pull $(IMAGE_NAME):latest
endif

help:

	@echo "Available targets:"
	@echo "  build        - Build the Docker image"
	@echo "  push         - Push the Docker image (use PUSH_DEVEL, PUSH_DEPLOY, or PUSH_LATEST to control which tags are pushed)"
	@echo "  pull         - Pull the Docker image (use PULL_DEVEL, PULL_DEPLOY, or PULL_LATEST to control which tags are pulled)"
	@echo "  help         - Show this help message"