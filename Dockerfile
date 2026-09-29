# Multi-stage build: one image containing both server binaries.
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.20
RUN adduser -D -u 10001 kv
COPY --from=build /out/ /usr/local/bin/
USER kv
# Protocol ports: coordinator 7100, nodes 7101-7104. HTTP admin = port + 1000.
EXPOSE 7100 7101 7102 7103 7104 8100 8101 8102 8103 8104
ENTRYPOINT []
