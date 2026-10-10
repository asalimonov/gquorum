FROM golang:1.26.5-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -trimpath -o /out/cluster ./examples/cluster
RUN mkdir -p /out/data && chmod 0700 /out/data

FROM scratch
COPY --from=build /out/cluster /cluster
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
EXPOSE 8080 9000
ENTRYPOINT ["/cluster"]
HEALTHCHECK --interval=2s --timeout=3s --start-period=5s --retries=20 CMD ["/cluster", "-healthcheck", "http://127.0.0.1:8080/health/ready"]
