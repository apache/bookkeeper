
VERSION ?= centos
IMAGE ?= bookkeeper/bookie:$(VERSION)
BOOKIE ?= 1
DOCKER_NETWORK ?= bk_network

BUILD_DIR ?= $(VERSION)

CONTAINER_NAME = bookkeeper-$(BOOKIE)
DOCKER_HOSTNAME = $(shell hostname)
BK_LOCAL_DATA_DIR = /tmp/test_bk
BK_LOCAL_CONTAINER_DATA_DIR = $(BK_LOCAL_DATA_DIR)/$(CONTAINER_NAME)
BK_DIR = /data
BK_LEDGERS_PATH = /ledgers

ZK_CONTAINER_NAME=test_zookeeper
ZK_LOCAL_DATA_DIR=$(BK_LOCAL_DATA_DIR)/zookkeeper


CONTAINER_IP=$(shell docker inspect --format '{{ .NetworkSettings.IPAddress }}' $(CONTAINER_NAME))

# -------------------------------- #

.PHONY: all build run create start stop shell exec root-shell root-exec info ip clean-files clean

# -------------------------------- #

all:
	make info

# -------------------------------- #

# Build the bookkeeper image.
#   make build
build:
	cd $(BUILD_DIR) ; \
	time docker build \
	    -t $(IMAGE) .

# -------------------------------- #

# Create and run a bookkeeper container with data persisted on local filesystem. It needs the zookkeeper container.
# In order to launch several bookies, the command need the bookie number
#   make run-bk BOOKIE=4

run-bk:
	mkdir -p $(BK_LOCAL_DATA_DIR) \
			$(BK_LOCAL_CONTAINER_DATA_DIR) \
			$(BK_LOCAL_CONTAINER_DATA_DIR)/journal \
			$(BK_LOCAL_CONTAINER_DATA_DIR)/ledger \
			$(BK_LOCAL_CONTAINER_DATA_DIR)/index
	
	-docker rm -f $(CONTAINER_NAME)
	docker run -it\
		--network $(DOCKER_NETWORK) \
	    --volume $(BK_LOCAL_CONTAINER_DATA_DIR)/journal:$(BK_DIR)/journal \
	    --volume $(BK_LOCAL_CONTAINER_DATA_DIR)/ledger:$(BK_DIR)/ledger \
	    --volume $(BK_LOCAL_CONTAINER_DATA_DIR)/index:$(BK_DIR)/index \
	    --name "$(CONTAINER_NAME)" \
	    --hostname "$(CONTAINER_NAME)" \
	    --env ZK_URL=$(ZK_CONTAINER_NAME):2181 \
	    --env BK_LEDGERS_PATH=$(BK_LEDGERS_PATH) \
	    $(IMAGE)

# -------------------------------- #

# Create run and destroy a container that will format zookkeeper metadata
#   make run-format

run-format:
	docker run -it --rm \
		--network $(DOCKER_NETWORK) \
		--env ZK_URL=$(ZK_CONTAINER_NAME):2181 \
		$(IMAGE) \
		bookkeeper shell metaformat $(FORMAT_OPTS)

# -------------------------------- #

# Create and run the zookkeeper container needed by the ensemble
#   make run-zk

run-zk:
	-docker network create $(DOCKER_NETWORK)
	mkdir -pv $(BK_LOCAL_DATA_DIR) $(ZK_LOCAL_DATA_DIR) $(ZK_LOCAL_DATA_DIR)/data $(ZK_LOCAL_DATA_DIR)/datalog
	-docker rm -f $(ZK_CONTAINER_NAME)
	docker run -it --rm \
		--network $(DOCKER_NETWORK) \
		--name "$(ZK_CONTAINER_NAME)" \
		--hostname "$(ZK_CONTAINER_NAME)" \
		-v $(ZK_LOCAL_DATA_DIR)/data:/data \
		-v $(ZK_LOCAL_DATA_DIR)/datalog:/datalog \
		-p 2181:2181 \
		zookeeper

# -------------------------------- #

# Create and run a container running the bookkeeper tutorial application (a simple dice rolling application).
# It's possible to run several dice applications in order to simulate a real life concurrent scenario.
#   make run-dice
run-dice:
	docker run -it --rm \
		--network $(DOCKER_NETWORK) \
		--env ZOOKEEPER_SERVERS=$(ZK_CONTAINER_NAME):2181 \
		caiok/bookkeeper-tutorial

# -------------------------------- #

# This is an example of a full bookkeeper ensemble of 3 bookies, a zookkeeper server and 2 client dice applications.
# On MacOS please run these command manually in several terminals
#   make run-demo
run-demo:
	$(eval WAIT_CMD := read -p 'Press Enter to close...')
	$(TERMINAL_EMULATOR) -e "bash -l -c \"make run-zk ; $(WAIT_CMD)"\"
	sleep 3
	#$(TERMINAL_EMULATOR) -e "bash -c \"make run-format FORMAT_OPTS=-nonInteractive ; make run-bk BOOKIE=1 ; $(WAIT_CMD)\""
	#sleep 3
	$(TERMINAL_EMULATOR) -e "bash -l -c \"make run-bk BOOKIE=1 TRY_METAFORMAT=true; $(WAIT_CMD)\""
	$(TERMINAL_EMULATOR) -e "bash -l -c \"make run-bk BOOKIE=2 TRY_METAFORMAT=true; $(WAIT_CMD)\""
	$(TERMINAL_EMULATOR) -e "bash -l -c \"make run-bk BOOKIE=3 TRY_METAFORMAT=true; $(WAIT_CMD)\""
	sleep 6
	$(TERMINAL_EMULATOR) -e "bash -l -c \"make run-dice ; $(WAIT_CMD)\""
	sleep 2
	$(TERMINAL_EMULATOR) -e "bash -l -c \"make run-dice ; $(WAIT_CMD)\""

	@echo
	@echo "If you want to restart from scratch the application, remove all its data:"
	@echo "  sudo rm -rf $(BK_LOCAL_DATA_DIR)"
	@echo

# -------------------------------- #
# Other undocumented utilities     #
# -------------------------------- #

start:
	docker start "$(CONTAINER_NAME)"

# -------------------------------- #

stop:
	docker stop "$(CONTAINER_NAME)"

# -------------------------------- #

shell exec:
	docker exec -it \
	    "$(CONTAINER_NAME)" \
	    /bin/bash -il

# -------------------------------- #

root-shell root-exec:
	docker exec -it "$(CONTAINER_NAME)" /bin/bash -il

# -------------------------------- #

info ip:
	@echo 
	@echo "Image: $(IMAGE)"
	@echo "Container name: $(CONTAINER_NAME)"
	@echo
	-@echo "Actual Image: $(shell docker inspect --format '{{ .RepoTags }} (created {{.Created }})' $(IMAGE))"
	-@echo "Actual Container: $(shell docker inspect --format '{{ .Name }} (created {{.Created }})' $(CONTAINER_NAME))"
	-@echo "Actual Container IP: $(shell docker inspect --format '{{ .NetworkSettings.IPAddress }}' $(CONTAINER_NAME))"
	@echo

# -------------------------------- #

clean-files:
	

clean:
	-docker stop $(CONTAINER_NAME)
	-docker rm $(CONTAINER_NAME)
	-docker rmi $(IMAGE)
	make clean-files
