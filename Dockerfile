# NEXUS-DISPATCH service image.
#
# One image, many entrypoints: each service is a binary under /app/bin,
# selected by the compose `command`. This keeps the build cache shared across
# all seven services.
#
#   docker build -t nexus-dispatch .
#   docker run nexus-dispatch /app/bin/ride-service

FROM golang:1.25-alpine AS build
WORKDIR /src
# Cache dependencies first (go.mod/go.sum rarely change).
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Static binaries, no debug info.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
      -o /out/bin/gateway            ./cmd/gateway \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
      -o /out/bin/driver-service     ./cmd/driver-service \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
      -o /out/bin/ride-service       ./cmd/ride-service \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
      -o /out/bin/location-service   ./cmd/location-service \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
      -o /out/bin/dispatch-engine    ./cmd/dispatch-engine \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
      -o /out/bin/event-processor    ./cmd/event-processor \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
      -o /out/bin/location-processor ./cmd/location-processor

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata \
 && adduser -D -u 10001 nexus
USER nexus
WORKDIR /app
COPY --from=build /out/bin /app/bin
# Default entrypoint; compose overrides `command` per service.
ENTRYPOINT ["/app/bin/gateway"]