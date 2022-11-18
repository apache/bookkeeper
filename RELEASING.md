# Bookkeeper release guide

## Docker

1. Check the latest GitHub release version

```bash
export BK_RELEASE_VERSION=`curl --silent https://api.github.com/repos/apache/bookkeeper/releases/latest | jq '.tag_name' | sed "s/\"release-\([0-9.]*\).*/\1/"`
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
```

from `docker` directory.

