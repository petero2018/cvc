
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

DOCKER_REPOSITORY=python
IMAGE_ARGS=DOCKER_REPOSITORY=${DOCKER_REPOSITORY} PYTHON_VERSION=${PYTHON_VERSION} PYTHON_FLAVOUR=${PYTHON_FLAVOUR} POETRY_VERSION=${POETRY_VERSION}
IMAGE_NAME_SHORT=cvc
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
	docker build $(IMAGE_ARGS_PR) -t $(IMAGE_NAME) .

login:
	@echo "Cleaning up old container (if any)..."
	-@docker rm -f cvc_container >nul 2>&1
	@echo "Starting container and auto-cding into /app/mini_data_warehouse..."
	docker run -it --name cvc_container python/cvc bash -c "cd /app/mini_data_warehouse && exec bash"
