# Integration tests for Bookkeeper

## Running the tests

### Workaround for running the tests with Mac Apple Silicon

The current version of the outdated maven plugin requires a workaround. 

```bash
brew install socat
socat TCP-LISTEN:2375,range=127.0.0.1/32,reuseaddr,fork UNIX-CLIENT:/var/run/docker.sock &
export DOCKER_HOST=tcp://localhost:2375
```

### Support for connecting to docker network from Mac host

```bash
brew install chipmk/tap/docker-mac-net-connect
sudo brew services start chipmk/tap/docker-mac-net-connect
```
See https://github.com/chipmk/docker-mac-net-connect for more details.

### Building the docker images

```bash
mvn -B -nsu clean install -Pdocker -DskipTests
docker images | grep apachebookkeeper
```
