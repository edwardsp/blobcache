FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    curl \
    ca-certificates \
    git \
    automake \
    autoconf \
    libtool \
    libfuse3-dev \
    libucx-dev \
    libibverbs-dev \
    librdmacm-dev \
    libclang-dev \
    && rm -rf /var/lib/apt/lists/*

ARG UCC_VERSION=v1.7.0
RUN git clone --depth 1 --branch ${UCC_VERSION} https://github.com/openucx/ucc.git /tmp/ucc && \
    cd /tmp/ucc && \
    ./autogen.sh && \
    ./configure --prefix=/usr/local --with-ucx=/usr && \
    make -j"$(nproc)" install && \
    ldconfig && \
    rm -rf /tmp/ucc

ENV PKG_CONFIG_PATH=/usr/local/lib/pkgconfig

ARG RUST_VERSION=stable
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --default-toolchain ${RUST_VERSION} --profile minimal
ENV PATH=/root/.cargo/bin:${PATH}
ENV CARGO_TERM_COLOR=always

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src
COPY examples ./examples

RUN cargo build --release --locked --features ucc

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    libfuse3-3 \
    fuse3 \
    libucx0 \
    libibverbs1 \
    librdmacm1t64 \
    ucx-utils \
    ibverbs-utils \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/libucc* /usr/local/lib/
COPY --from=builder /usr/local/lib/ucc /usr/local/lib/ucc
RUN ldconfig
COPY --from=builder /src/target/release/blobcached /usr/local/bin/blobcached

ENTRYPOINT ["/usr/local/bin/blobcached"]
CMD ["--config", "/etc/blobcached.toml"]
