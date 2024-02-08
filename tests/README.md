# Integration tests for Bookkeeper

## Prerequisites

### Workaround for running the tests with Mac Apple Silicon

The current version of the outdated maven plugin requires a workaround. 

Install socat
```bash
brew install socat
```

Run a TCP -> Unix socket proxy for docker in a separate terminal
```bash
socat TCP-LISTEN:2375,bind=127.0.0.1,reuseaddr,fork UNIX-CLIENT:/var/run/docker.sock &
```

Set the `DOCKER_HOST` environment variable in the terminal where you run the tests
```bash
export DOCKER_HOST=tcp://localhost:2375
```

Here's a shell script function to automate starting the proxy and setting `DOCKER_HOST`:
```bash
function docker_socket_proxy() {
  local port=2375
  socat /dev/null TCP4:127.0.0.1:$port,connect-timeout=2 &> /dev/null
  if [ $? -ne 0 ]; then
    echo "Starting socat tcp proxy on port $port for docker socket /var/run/docker.sock"
    socat TCP-LISTEN:$port,bind=127.0.0.1,reuseaddr,fork UNIX-CLIENT:/var/run/docker.sock &> /dev/null &
    echo "Stop the proxy with 'kill $!'"
  fi
  export DOCKER_HOST=tcp://127.0.0.1:$port
  echo "Added DOCKER_HOST=$DOCKER_HOST to environment"
}
```
You can add the function to your shell profile (`~/.bashrc`, `~/.zshrc`, etc.).

### Support for connecting to docker network from Mac host

You will also need to install [docker-mac-net-connect](https://github.com/chipmk/docker-mac-net-connect). It allows the tests to connect to the docker network from the Mac host.

```bash
brew install chipmk/tap/docker-mac-net-connect
sudo brew services start chipmk/tap/docker-mac-net-connect
```

## Running the tests

Remember to start the unix socket proxy for docker as described in the previous section and set the `DOCKER_HOST` environment variable in the terminal where you run the tests.

### Building the docker images together with the project source code

```bash
mvn -B -nsu clean install -Pdocker -DskipTests
docker images | grep apachebookkeeper
```

### Running integration tests

```bash
# Run metadata driver tests
mvn -f metadata-drivers/pom.xml test -DintegrationTests -Dorg.slf4j.simpleLogger.defaultLogLevel=INFO -DredirectTestOutputToFile=false -DtestRetryCount=0

# Run all integration tests
mvn -f tests/pom.xml test -DintegrationTests -Dorg.slf4j.simpleLogger.defaultLogLevel=INFO -DredirectTestOutputToFile=false -DtestRetryCount=0
```

### Running backward compatibility tests

```bash
# Test current server with old clients
mvn -DbackwardCompatTests -pl :backward-compat-current-server-old-clients test -Dorg.slf4j.simpleLogger.defaultLogLevel=INFO -DredirectTestOutputToFile=false -DtestRetryCount=0

# Test progressive upgrade
mvn -DbackwardCompatTests -pl :upgrade test -Dorg.slf4j.simpleLogger.defaultLogLevel=INFO -DredirectTestOutputToFile=false -DtestRetryCount=0

# Other backward compat tests
mvn -DbackwardCompatTests -pl :bc-non-fips,:hierarchical-ledger-manager,:hostname-bookieid,:old-cookie-new-cluster,:recovery-no-password,:upgrade-direct,:yahoo-custom-version test -Dorg.slf4j.simpleLogger.defaultLogLevel=INFO -DredirectTestOutputToFile=false -DtestRetryCount=0
```