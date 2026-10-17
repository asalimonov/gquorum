FROM golang:1.26.5-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY examples/inmemory-cache/go.mod examples/inmemory-cache/go.sum ./examples/inmemory-cache/
RUN --mount=type=cache,target=/go/pkg/mod go -C examples/inmemory-cache mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go -C examples/inmemory-cache build -trimpath -o /out/cache-server ./server && \
    CGO_ENABLED=0 go -C examples/inmemory-cache build -trimpath -o /out/cache-loadgen ./loadgen
RUN mkdir -p /out/data && chmod 0700 /out/data

FROM scratch
COPY --from=build /out/cache-server /cache-server
COPY --from=build /out/cache-loadgen /cache-loadgen
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
EXPOSE 6379 8080 9000 9001
ENTRYPOINT ["/cache-server"]
