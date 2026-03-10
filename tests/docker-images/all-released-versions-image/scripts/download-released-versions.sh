#!/usr/bin/env bash
#
#/**
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# Downloads all released BookKeeper versions and GPG key files to DEST_DIR.
# Skips files that already exist (idempotent).
#
# Usage: download-released-versions.sh [DEST_DIR]
#   DEST_DIR defaults to /released-versions

if [ "${CURRENT_VERSION_ONLY}" = "true" ]; then
  echo "Skipping downloading of old BookKeeper versions..."
  exit 0
fi

set -e

DEST_DIR="${1:-/released-versions}"
mkdir -p "$DEST_DIR"

if command -v curl &>/dev/null; then
    # --retry 5                   : retry up to 5 times on transient failures
    # --retry-delay 5             : wait 5 s between retries
    # --connect-timeout 30        : fail if the connection cannot be established within 30 s
    # --speed-limit 1 --speed-time 15 : abort (and retry) if the transfer is idle for 15 s
    _fetch() { curl -fsSL --retry 5 --retry-delay 5 --connect-timeout 30 --speed-limit 1 --speed-time 15 "$1" -o "$2"; }
else
    _fetch() { wget -nv "$1" -O "$2"; }
fi

download_if_missing() {
    local url="$1"
    local filename="$2"
    local dest="$DEST_DIR/$filename"
    if [ ! -f "$dest" ]; then
        echo "Downloading $filename ..."
        _fetch "$url" "$dest"
    else
        echo "Skipping $filename (already exists)"
    fi
}

download_tarball() {
    local base_url="$1"
    local tarball="$2"
    shift 2
    # remaining args are checksum/signature extensions
    download_if_missing "$base_url/$tarball" "$tarball"
    for ext in "$@"; do
        download_if_missing "$base_url/$tarball.$ext" "$tarball.$ext"
    done
}

# 4.1.0 – 4.2.0 (old path under zookeeper/)
download_tarball \
    "https://archive.apache.org/dist/zookeeper/bookkeeper/bookkeeper-4.1.0" \
    "bookkeeper-server-4.1.0-bin.tar.gz" sha1 md5 asc
download_tarball \
    "https://archive.apache.org/dist/zookeeper/bookkeeper/bookkeeper-4.2.0" \
    "bookkeeper-server-4.2.0-bin.tar.gz" sha1 md5 asc

# 4.8.2 and later (canonical bookkeeper/ path)
download_tarball \
    "https://archive.apache.org/dist/bookkeeper/bookkeeper-4.8.2" \
    "bookkeeper-server-4.8.2-bin.tar.gz" sha512 asc
download_tarball \
    "https://archive.apache.org/dist/bookkeeper/bookkeeper-4.9.2" \
    "bookkeeper-server-4.9.2-bin.tar.gz" sha512 asc
download_tarball \
    "https://archive.apache.org/dist/bookkeeper/bookkeeper-4.10.0" \
    "bookkeeper-server-4.10.0-bin.tar.gz" sha512 asc
download_tarball \
    "https://archive.apache.org/dist/bookkeeper/bookkeeper-4.11.1" \
    "bookkeeper-server-4.11.1-bin.tar.gz" sha512 asc
download_tarball \
    "https://archive.apache.org/dist/bookkeeper/bookkeeper-4.12.1" \
    "bookkeeper-server-4.12.1-bin.tar.gz" sha512 asc
download_tarball \
    "https://archive.apache.org/dist/bookkeeper/bookkeeper-4.13.0" \
    "bookkeeper-server-4.13.0-bin.tar.gz" sha512 asc
download_tarball \
    "https://archive.apache.org/dist/bookkeeper/bookkeeper-4.14.8" \
    "bookkeeper-server-4.14.8-bin.tar.gz" sha512 asc
download_tarball \
    "https://archive.apache.org/dist/bookkeeper/bookkeeper-4.15.5" \
    "bookkeeper-server-4.15.5-bin.tar.gz" sha512 asc
download_tarball \
    "https://archive.apache.org/dist/bookkeeper/bookkeeper-4.16.7" \
    "bookkeeper-server-4.16.7-bin.tar.gz" sha512 asc
download_tarball \
    "https://archive.apache.org/dist/bookkeeper/bookkeeper-4.17.2" \
    "bookkeeper-server-4.17.2-bin.tar.gz" sha512 asc

# GPG key files
download_if_missing \
    "https://dist.apache.org/repos/dist/release/bookkeeper/KEYS" \
    "KEYS"
download_if_missing \
    "http://svn.apache.org/repos/asf/zookeeper/bookkeeper/dist/KEYS?p=1620552" \
    "KEYS.old"
