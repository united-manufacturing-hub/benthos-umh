FROM golang:1.17 as build

RUN useradd -u 10001 benthos

WORKDIR /go/src/github.com/makenew/benthos-plugin

COPY go.mod go.sum ./
RUN go mod download

COPY ./Makefile .
COPY ./cmd ./cmd
COPY ./plugin ./plugin

RUN CGO_ENABLED=0 GOOS=linux go build cmd/benthos/main.go

FROM busybox

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
