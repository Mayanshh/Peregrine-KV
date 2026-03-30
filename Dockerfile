FROM golang:1.22-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build a static-ish binary for the gRPC server.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/peregrinekv-server ./cmd/server

FROM alpine:3.20

RUN adduser -D -u 10001 appuser
USER appuser
WORKDIR /app

COPY --from=builder /app/peregrinekv-server /app/peregrinekv-server

# Default ports:
# - kv-addr: 50051
# - raft-addr: 25051 (if raft enabled)
EXPOSE 50051 25051

ENTRYPOINT ["/app/peregrinekv-server"]

