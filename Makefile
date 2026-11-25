APP_NAME := taskforge
IMAGE_NAME := taskforge-api:local
COMPOSE := docker compose
TEST_COMPOSE := docker compose -f docker-compose.test.yml
GO_PACKAGES := ./...
GOCACHE ?= $(CURDIR)/.cache/go-build
GOMODCACHE ?= $(CURDIR)/.cache/go-mod
GOENV := GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE)

TEMPL_VERSION := v0.3.1118
TAILWIND_VERSION := 4.3.5
TAILWIND := bin/tailwindcss-$(TAILWIND_VERSION)
UNAME_S := $(shell uname -s | tr '[:upper:]' '[:lower:]')
UNAME_M := $(shell uname -m)
TAILWIND_OS := $(if $(filter darwin,$(UNAME_S)),macos,linux)
TAILWIND_ARCH := $(if $(filter arm64 aarch64,$(UNAME_M)),arm64,x64)

TEST_DATABASE_URL := postgres://taskforge:taskforge@localhost:55432/taskforge_test?sslmode=disable
TEST_REDIS_URL := redis://localhost:56379/0

.PHONY: help build run worker test test-integration generate templ css tidy docker-build up down restart logs ps clean

help:
	@printf "Targets:\n"
	@printf "  build             Build the API and worker binaries into ./bin\n"
	@printf "  run               Run the API locally\n"
	@printf "  worker            Run the reference worker locally\n"
	@printf "  test              Run unit tests (Postgres/Redis tests are skipped)\n"
	@printf "  test-integration  Run every test against disposable Postgres and Redis\n"
	@printf "  generate          Regenerate templ components and the dashboard stylesheet\n"
	@printf "  templ             Regenerate templ components only\n"
	@printf "  css               Rebuild web/admin/static/app.css with Tailwind\n"
	@printf "  tidy              Tidy Go modules\n"
	@printf "  docker-build      Build the Docker image\n"
	@printf "  up / down         Start or stop the Docker Compose stack\n"
	@printf "  restart           Restart the Docker Compose stack\n"
	@printf "  logs / ps         Tail logs or list Compose services\n"
	@printf "  clean             Remove local build output\n"

build:
	@mkdir -p bin
	$(GOENV) go build -trimpath -o bin/$(APP_NAME) ./cmd/taskforge
	$(GOENV) go build -trimpath -o bin/worker ./cmd/worker

run:
	$(GOENV) go run ./cmd/taskforge

worker:
	$(GOENV) go run ./cmd/worker

test:
	$(GOENV) go test $(GO_PACKAGES)

# Integration packages share one database, so packages run one at a time.
test-integration:
	$(TEST_COMPOSE) up -d --wait
	TASKFORGE_TEST_DATABASE_URL='$(TEST_DATABASE_URL)' TASKFORGE_TEST_REDIS_URL='$(TEST_REDIS_URL)' \
		$(GOENV) go test -p 1 -count=1 $(GO_PACKAGES); \
		status=$$?; $(TEST_COMPOSE) down; exit $$status

generate: templ css

templ:
	go run github.com/a-h/templ/cmd/templ@$(TEMPL_VERSION) generate -path internal/admin/views

$(TAILWIND):
	@mkdir -p bin
	curl -fsSL -o $@ https://github.com/tailwindlabs/tailwindcss/releases/download/v$(TAILWIND_VERSION)/tailwindcss-$(TAILWIND_OS)-$(TAILWIND_ARCH)
	chmod +x $@

css: $(TAILWIND)
	$(TAILWIND) -i web/admin/tailwind.css -o web/admin/static/app.css --minify

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
