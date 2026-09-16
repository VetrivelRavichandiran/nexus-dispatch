# NEXUS-DISPATCH developer Makefile.
#
# Common targets:
#   make build     - build all service binaries into ./bin
#   make test      - run unit + integration tests (in-memory Redis)
#   make vet       - go vet
#   make up        - docker compose up --build (full stack)
#   make down      - docker compose down
#   make demo      - seed a demo driver + create a ride via the gateway

BIN      := bin
SERVICES := gateway driver-service ride-service location-service \
            dispatch-engine event-processor location-processor
GO       ?= go

.PHONY: all
all: build

.PHONY: build
build:
	@mkdir -p $(BIN)
	@for s in $(SERVICES); do \
		echo "  building $$s"; \
		$(GO) build -o $(BIN)/$$s ./cmd/$$s || exit 1; \
	done
	@echo "built: $(BIN)/{$(SERVICES)}"

.PHONY: test
test:
	$(GO) test ./... -count=1

.PHONY: test-race
test-race:
	$(GO) test -race ./... -count=1

.PHONY: vet
vet:
	$(GO) vet ./...

.PHONY: fmt
fmt:
	$(GO) fmt ./...

.PHONY: tidy
tidy:
	$(GO) mod tidy

.PHONY: up
up:
	docker compose up --build -d

.PHONY: down
down:
	docker compose down

.PHONY: logs
logs:
	docker compose logs -f

# Demo: register a driver, then request a ride near it (Bengaluru).
.PHONY: demo
demo:
	@TOKEN=$$(curl -s -X POST localhost:8080/api/v1/auth/login \
		-H 'Content-Type: application/json' \
		-d '{"email":"driver1@example.com","role":"driver"}' | sed -n 's/.*"token":"\([^"]*\)".*/\1/p'); \
	echo "driver token: $$TOKEN"; \
	curl -s -X POST localhost:8080/api/v1/drivers \
		-H "Authorization: Bearer $$TOKEN" -H 'Content-Type: application/json' \
		-d '{"name":"Demo Driver","email":"driver1@example.com","lat":12.9716,"lng":77.5946,"vehicle":{"make":"Toyota","model":"Corolla","plate":"KA-01-AB-1234","seats":4}}'; echo; \
	UTOKEN=$$(curl -s -X POST localhost:8080/api/v1/auth/login \
		-H 'Content-Type: application/json' \
		-d '{"email":"user1@example.com","role":"user"}' | sed -n 's/.*"token":"\([^"]*\)".*/\1/p'); \
	curl -s -X POST localhost:8080/api/v1/rides \
		-H "Authorization: Bearer $$UTOKEN" -H 'Content-Type: application/json' \
		-d '{"pickup_lat":12.9720,"pickup_lng":77.5950,"dropoff_lat":12.9800,"dropoff_lng":77.6100,"radius_m":1500}'; echo

.PHONY: clean
clean:
	rm -rf $(BIN)