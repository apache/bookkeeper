/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.zookeeper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watcher for receiving zookeeper server connection events.
 */
public class ZooKeeperWatcherBase implements Watcher {
    private static final Logger LOG = LoggerFactory
            .getLogger(ZooKeeperWatcherBase.class);

    private final int zkSessionTimeOut;
    private CountDownLatch clientConnectLatch = new CountDownLatch(1);

    public ZooKeeperWatcherBase(int zkSessionTimeOut) {
        this.zkSessionTimeOut = zkSessionTimeOut;
    }

    @Override
    public void process(WatchedEvent event) {
        // If event type is NONE, this is a connection status change
        if (event.getType() != EventType.None) {
            LOG.debug("Recieved event: {}, path: {} from ZooKeeper server",
                    event.getType(), event.getPath());
            return;
        }

        LOG.debug("Recieved {} from ZooKeeper server", event.getState());
        // TODO: Needs to handle AuthFailed, SaslAuthenticated events
        switch (event.getState()) {
        case SyncConnected:
            clientConnectLatch.countDown();
            break;
        case Disconnected:
            LOG.debug("Ignoring Disconnected event from ZooKeeper server");
            break;
        case Expired:
            LOG.error("ZooKeeper client connection to the "
                    + "ZooKeeper server has expired!");
            break;
        }
    }

    /**
     * Waiting for the SyncConnected event from the ZooKeeper server
     * 
     * @throws KeeperException
     *             when there is no connection
     * @throws InterruptedException
     *             interrupted while waiting for connection
     */
    public void waitForConnection() throws KeeperException,
            InterruptedException {
        if (!clientConnectLatch.await(zkSessionTimeOut, TimeUnit.MILLISECONDS)) {
            throw KeeperException.create(KeeperException.Code.CONNECTIONLOSS);
        }
    }

    /**
     * Return zookeeper session time out
     */
    public int getZkSessionTimeOut() {
        return zkSessionTimeOut;
    }

}
