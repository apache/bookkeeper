#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

FROM --platform=linux/amd64 maven:3.9.11-eclipse-temurin-17
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
  apt-get dist-upgrade -y && \
  apt-get install -y g++ cmake gnupg2 vim subversion less zip unzip xz-utils

# Rust toolchain (for native-io module's cargo-zigbuild build)
ENV RUSTUP_HOME=/usr/local/rustup \
  CARGO_HOME=/usr/local/cargo \
  PATH=/usr/local/cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --default-toolchain stable --profile minimal --no-modify-path && \
  rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu && \
  chmod -R a+rwX /usr/local/rustup /usr/local/cargo

# Zig (used by cargo-zigbuild for cross-compilation)
ARG ZIG_VERSION=0.13.0
RUN curl -sL https://ziglang.org/download/${ZIG_VERSION}/zig-linux-x86_64-${ZIG_VERSION}.tar.xz | \
    tar -xJ -C /opt && \
  ln -s /opt/zig-linux-x86_64-${ZIG_VERSION}/zig /usr/local/bin/zig

# cargo-zigbuild
RUN cargo install cargo-zigbuild --locked && \
  chmod -R a+rwX /usr/local/cargo

