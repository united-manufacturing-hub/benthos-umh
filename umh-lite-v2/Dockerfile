FROM golang:1.23-alpine as build

WORKDIR /go/src/github.com/united-manufacturing-hub/benthos-umh

# Install build dependencies
RUN apk add --no-cache \
    gcc \
    musl-dev

# Copy go.mod first for better caching
COPY ./umh-lite-v2/go.mod ./umh-lite-v2/
WORKDIR /go/src/github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2
RUN go mod download

# Copy and build umh-lite-v2
COPY ./umh-lite-v2 .
RUN CGO_ENABLED=0 go build \
    -ldflags "-s -w" \
    -o umh-lite-v2 \
    cmd/main.go

FROM alpine:3.18

# Install basic dependencies
RUN apk add --no-cache \
    bash \
    curl \
    jq \
    tzdata \
    ca-certificates \
    xz

# Set versions
ARG S6_OVERLAY_VERSION=3.2.0.2
ARG BENTHOS_UMH_VERSION=0.6.2
ARG TARGETARCH=amd64

# Step 1: Install s6-overlay noarch components
ADD https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-noarch.tar.xz /tmp/
RUN tar -C / -Jxpf /tmp/s6-overlay-noarch.tar.xz && rm /tmp/s6-overlay-noarch.tar.xz

# Step 2: Install architecture-specific s6-overlay components
RUN case "${TARGETARCH}" in \
    "amd64") S6_ARCH="x86_64" ;; \
    "arm64") S6_ARCH="aarch64" ;; \
    "arm/v7") S6_ARCH="arm" ;; \
    *) echo "Unsupported architecture: ${TARGETARCH}" && exit 1 ;; \
    esac && \
    echo "Using S6_ARCH: ${S6_ARCH}" && \
    wget -O /tmp/s6-overlay-arch.tar.xz \
    "https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-${S6_ARCH}.tar.xz" && \
    tar -C / -Jxpf /tmp/s6-overlay-arch.tar.xz && \
    rm /tmp/s6-overlay-arch.tar.xz

# Step 3: Install syslogd components
ADD https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/syslogd-overlay-noarch.tar.xz /tmp/
RUN tar -C / -Jxpf /tmp/syslogd-overlay-noarch.tar.xz && rm /tmp/syslogd-overlay-noarch.tar.xz

# Step 4: Install Benthos
RUN case "${TARGETARCH}" in \
    "amd64") BENTHOS_ARCH="amd64" ;; \
    "arm64") BENTHOS_ARCH="arm64" ;; \
    "arm/v7") BENTHOS_ARCH="armv7" ;; \
    *) echo "Unsupported architecture: ${TARGETARCH}" && exit 1 ;; \
    esac && \
    echo "Using BENTHOS_ARCH: ${BENTHOS_ARCH}" && \
    curl -sSL -o /usr/local/bin/benthos \
    "https://github.com/united-manufacturing-hub/benthos-umh/releases/download/v${BENTHOS_UMH_VERSION}/benthos-linux-${BENTHOS_ARCH}" && \
    chmod +x /usr/local/bin/benthos

# Step 5: Copy the built application and set permissions
COPY --from=build /go/src/github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/umh-lite-v2 /usr/local/bin/umh-lite-v2
RUN chmod +x /usr/local/bin/umh-lite-v2 && \
    ls -la /usr/local/bin/umh-lite-v2 && \
    test -x /usr/local/bin/umh-lite-v2 || (echo "Binary not executable" && exit 1) 

# Step 6: Copy s6 service configuration
COPY ./umh-lite-v2/s6-base/s6-rc.d /etc/s6-overlay/s6-rc.d/
RUN chmod +x /etc/s6-overlay/s6-rc.d/umh-lite-v2/run 

# Step 7: Set environment variables
ENV S6_KEEP_ENV=1 \
    S6_BEHAVIOUR_IF_STAGE2_FAILS=2 \
    S6_CMD_WAIT_FOR_SERVICES=1 \
    S6_SERVICES_READYTIME=50

VOLUME [ "/data" ]

# Step 8: Set entrypoint
ENTRYPOINT ["/init"]
CMD []
