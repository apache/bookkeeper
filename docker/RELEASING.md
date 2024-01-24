<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Bookkeeper release guide

## Docker

1. Check the latest GitHub release version

    ```bash
    export BK_RELEASE_VERSION=`curl -s https://api.github.com/repos/apache/bookkeeper/releases/latest | jq -r '.tag_name' | sed "s/release-\([0-9.]*\).*/\1/"`
    echo $BK_RELEASE_VERSION
    ```

2. Build Docker image

    ```bash
    docker rmi apache/bookkeeper:latest --force
    docker build --build-arg BK_VERSION=$BK_RELEASE_VERSION . -t apache/bookkeeper:latest
    ```

    from `docker` directory.

3. Verify the image libs

    ```bash
    docker run apache/bookkeeper -c "ls lib"
    ```

4. Build & publish for all platforms

    ```bash
    docker buildx create --name bookkeeperbuilder --use --bootstrap
    docker buildx build --platform=linux/amd64,linux/arm64 --build-arg BK_VERSION=$BK_RELEASE_VERSION --push . -t apache/bookkeeper:$BK_RELEASE_VERSION
    docker buildx rm bookkeeperbuilder
    ```
    
    from `docker` directory.

