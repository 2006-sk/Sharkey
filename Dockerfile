# Multi-stage build: one image containing both server binaries.
#
# Stage 1 ("build") has the full Go toolchain and compiles the code. Stage 2
# is a tiny Alpine image that receives only the compiled binaries. The final
# image therefore contains no compiler, no source and no module cache:
# smaller to ship and less to attack. (All four commands under cmd/ are
# built; the client is used by the compose health checks.)
FROM golang:1.22-alpine AS build
WORKDIR /src
# Copy go.mod alone first and download modules: Docker caches each layer, so
# as long as go.mod is unchanged this step is reused and a source edit only
# re-runs the steps below. (The project has no third-party dependencies, but
# the pattern costs nothing.)
COPY go.mod ./
RUN go mod download
COPY . .
# CGO_ENABLED=0 -> a fully static binary that runs on Alpine's musl libc.
# -trimpath removes local file-system paths from the binary;
# -ldflags="-s -w" strips the symbol table and DWARF debug info (smaller).
# ./cmd/... builds every command into /out/.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

# Stage 2: the runtime image.
FROM alpine:3.20
# Run as an unprivileged user (uid 10001) instead of root, so a bug in the
# server cannot act as root inside the container. Ports >1024 need no root.
RUN adduser -D -u 10001 kv
# /usr/local/bin is on PATH, so compose can run "node", "coordinator" and
# "client" by name.
COPY --from=build /out/ /usr/local/bin/
USER kv
# Protocol ports: coordinator 7100, nodes 7101-7104. HTTP admin = port + 1000.
# (EXPOSE is documentation; the actual host mapping is in docker-compose.yml.)
EXPOSE 7100 7101 7102 7103 7104 8100 8101 8102 8103 8104
# No default program: each compose service chooses it with `command:`
# (node or coordinator), so one image serves both roles.
ENTRYPOINT []
