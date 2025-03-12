FROM alpine:3.18

ARG S6_OVERLAY_VERSION=3.2.0.3
ARG TARGETARCH=amd64

# Install dependencies
RUN apk add --no-cache \
    bash \
    curl \
    jq \
    tzdata \
    ca-certificates \
    xz

# Install s6-overlay for process supervision
RUN case ${TARGETARCH} in \
        "amd64") S6_ARCH="x86_64" ;; \
        "arm64") S6_ARCH="aarch64" ;; \
        "arm/v7") S6_ARCH="arm" ;; \
        "arm/v6") S6_ARCH="armhf" ;; \
        "386") S6_ARCH="i686" ;; \
        "riscv64") S6_ARCH="riscv64" ;; \
        "s390x") S6_ARCH="s390x" ;; \
        *) echo "Unsupported architecture: ${TARGETARCH}" && exit 1 ;; \
    esac && \
    curl -sSL https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-noarch.tar.xz | tar -Jxpf - -C / && \
    curl -sSL https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-${S6_ARCH}.tar.xz | tar -Jxpf - -C / && \
    curl -sSL https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/syslogd-overlay-noarch.tar.xz | tar -Jxpf - -C /

# Install Benthos
ARG BENTHOS_VERSION=4.24.0
RUN curl -sSL https://github.com/benthosdev/benthos/releases/download/v${BENTHOS_VERSION}/benthos_${BENTHOS_VERSION}_linux_${TARGETARCH}.tar.gz | tar -xz -C /usr/local/bin benthos

# Install Redpanda (lightweight version for edge devices)
ARG REDPANDA_VERSION=23.3.5
RUN mkdir -p /tmp/redpanda && \
    curl -sSL https://github.com/redpanda-data/redpanda/releases/download/v${REDPANDA_VERSION}/rpk-linux-$([ "$TARGETARCH" = "amd64" ] && echo "amd64" || echo "arm64").tar.gz | tar -xz -C /tmp/redpanda && \
    mv /tmp/redpanda/rpk /usr/local/bin/ && \
    rm -rf /tmp/redpanda

# Create necessary directories
RUN mkdir -p /data/configs /data/redpanda /data/state

# Copy the application
COPY ./umh-lite-v2/cmd/umh-lite-v2 /usr/local/bin/

# Copy s6-overlay service definitions
COPY ./umh-lite-v2/s6-overlay/ /etc/

# Set environment variables
ENV S6_KEEP_ENV=1 \
    S6_BEHAVIOUR_IF_STAGE2_FAILS=2 \
    S6_CMD_WAIT_FOR_SERVICES=1

# Data volume
VOLUME ["/data"]

# Expose ports
EXPOSE 4195 9092 8080

# Set s6-overlay as the entrypoint
ENTRYPOINT ["/init"]

# Command will be defined by s6-overlay services
CMD []
