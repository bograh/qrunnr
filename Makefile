APP_NAME := taskforge
IMAGE_NAME := taskforge-api:local
COMPOSE := docker compose
GO_PACKAGES := ./...
GOCACHE ?= $(CURDIR)/.cache/go-build
GOMODCACHE ?= $(CURDIR)/.cache/go-mod
GOENV := GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE)

.PHONY: help build run test tidy docker-build up down restart logs ps clean

help:
	@printf "Targets:\n"
	@printf "  build         Build the API binary into ./bin/%s\n" "$(APP_NAME)"
	@printf "  run           Run the API locally\n"
	@printf "  test          Run Go tests\n"
	@printf "  tidy          Tidy Go modules\n"
	@printf "  docker-build  Build the Docker image\n"
	@printf "  up            Start the Docker Compose stack\n"
	@printf "  down          Stop the Docker Compose stack\n"
	@printf "  restart       Restart the Docker Compose stack\n"
	@printf "  logs          Tail Docker Compose logs\n"
	@printf "  ps            Show Docker Compose services\n"
	@printf "  clean         Remove local build output\n"

build:
	@mkdir -p bin
	$(GOENV) go build -trimpath -o bin/$(APP_NAME) ./cmd/taskforge

run:
	$(GOENV) go run ./cmd/taskforge

test:
	$(GOENV) go test $(GO_PACKAGES)

tidy:
	$(GOENV) go mod tidy

docker-build:
	$(COMPOSE) build api

up:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) down
	$(COMPOSE) up --build -d

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

clean:
	@if [ -d .cache ]; then chmod -R u+w .cache; fi
	rm -rf bin .cache
