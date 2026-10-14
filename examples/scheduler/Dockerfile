FROM golang:1.26.5-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY examples/scheduler/go.mod examples/scheduler/go.sum ./examples/scheduler/
RUN --mount=type=cache,target=/go/pkg/mod go -C examples/scheduler mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go -C examples/scheduler build -trimpath -o /out/scheduler .
RUN mkdir -p /out/data && chmod 0700 /out/data

FROM scratch
COPY --from=build /out/scheduler /scheduler
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
EXPOSE 8080 9000 9001
ENTRYPOINT ["/scheduler"]
HEALTHCHECK --interval=2s --timeout=3s --start-period=10s --retries=40 CMD ["/scheduler", "healthcheck", "http://127.0.0.1:8080/healthz"]
