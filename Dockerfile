FROM golang:1.18 as build

RUN useradd -u 10001 benthos

RUN echo 'deb [trusted=yes] https://repo.goreleaser.com/apt/ /' \
  | tee /etc/apt/sources.list.d/goreleaser.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends goreleaser \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /go/src/github.com/makenew/benthos-plugin

COPY go.mod go.sum ./
RUN go mod download

COPY ./cmd ./cmd
COPY ./plugin ./plugin
COPY .goreleaser.yml .
RUN echo 'project_name: app' >> .goreleaser.yml
RUN goreleaser build --single-target --snapshot --id benthos --output ./main

FROM busybox as app

WORKDIR /

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /go/src/github.com/makenew/benthos-plugin/main benthos
COPY ./config/default.yaml /benthos.yaml

ENTRYPOINT ["/benthos"]

CMD ["-c", "/benthos.yaml"]

EXPOSE 4195

USER benthos

LABEL org.opencontainers.image.source https://github.com/makenew/benthos-plugin
