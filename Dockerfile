# Stage 1: Build Go binary
FROM golang:1.26-bookworm AS go-builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /bridge ./cmd/bridge

# Stage 2: Install Node.js sidecar dependencies
FROM node:22-bookworm-slim AS node-builder
WORKDIR /sidecar
COPY sidecar/package.json ./
COPY sidecar/package-lock.json ./
RUN npm ci --omit=dev

# Stage 3: Runtime
FROM node:22-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Go binary
COPY --from=go-builder /bridge /app/bridge

# Node.js sidecar
COPY sidecar/index.mjs /app/sidecar/
COPY --from=node-builder /sidecar/node_modules /app/sidecar/node_modules
COPY sidecar/package.json /app/sidecar/

# Default config location
COPY config.example.yaml /app/config.example.yaml

# Database and socket stored in /data (mount as volume for persistence)
ENV BRIDGE_DATABASE=/data/bridge.db
ENV IPC_SOCKET_PATH=/tmp/discord-voice-bridge.sock
VOLUME /data

ENTRYPOINT ["/app/bridge"]
CMD ["-config", "/data/config.yaml"]
