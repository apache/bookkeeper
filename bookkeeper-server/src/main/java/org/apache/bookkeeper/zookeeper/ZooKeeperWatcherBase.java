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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
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
    private volatile CountDownLatch clientConnectLatch = new CountDownLatch(1);
    private final CopyOnWriteArraySet<Watcher> childWatchers =
            new CopyOnWriteArraySet<Watcher>();
    private final StatsLogger statsLogger;
    private final ConcurrentHashMap<Event.KeeperState, Counter> stateCounters =
            new ConcurrentHashMap<Event.KeeperState, Counter>();
    private final ConcurrentHashMap<EventType, Counter> eventCounters =
            new ConcurrentHashMap<EventType, Counter>();

    public ZooKeeperWatcherBase(int zkSessionTimeOut) {
        this(zkSessionTimeOut, NullStatsLogger.INSTANCE);
    }

    public ZooKeeperWatcherBase(int zkSessionTimeOut, StatsLogger statsLogger) {
        this(zkSessionTimeOut, new HashSet<Watcher>(), statsLogger);
    }

    public ZooKeeperWatcherBase(int zkSessionTimeOut,
                                Set<Watcher> childWatchers,
                                StatsLogger statsLogger) {
        this.zkSessionTimeOut = zkSessionTimeOut;
        this.childWatchers.addAll(childWatchers);
        this.statsLogger = statsLogger;
    }

    public ZooKeeperWatcherBase addChildWatcher(Watcher watcher) {
        this.childWatchers.add(watcher);
        return this;
    }

    public ZooKeeperWatcherBase removeChildWatcher(Watcher watcher) {
        this.childWatchers.remove(watcher);
        return this;
    }

    private Counter getEventCounter(EventType type) {
        Counter c = eventCounters.get(type);
        if (null == c) {
            Counter newCounter = statsLogger.scope("events").getCounter(type.name());
            Counter oldCounter = eventCounters.putIfAbsent(type, newCounter);
            if (null != oldCounter) {
                c = oldCounter;
            } else {
                c = newCounter;
            }
        }
        return c;
    }

    public Counter getStateCounter(Event.KeeperState state) {
        Counter c = stateCounters.get(state);
        if (null == c) {
            Counter newCounter = statsLogger.scope("state").getCounter(state.name());
            Counter oldCounter = stateCounters.putIfAbsent(state, newCounter);
            if (null != oldCounter) {
                c = oldCounter;
            } else {
                c = newCounter;
            }
        }
        return c;
    }

    @Override
    public void process(WatchedEvent event) {
        // If event type is NONE, this is a connection status change
        if (event.getType() != EventType.None) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Received event: {}, path: {} from ZooKeeper server", event.getType(), event.getPath());
            }
            getEventCounter(event.getType()).inc();
            // notify the child watchers
            notifyEvent(event);
            return;
        }
        getStateCounter(event.getState()).inc();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received {} from ZooKeeper server", event.getState());
        }
        // TODO: Needs to handle AuthFailed, SaslAuthenticated events
        //       {@link https://github.com/apache/bookkeeper/issues/284}
        switch (event.getState()) {
        case SyncConnected:
            LOG.info("ZooKeeper client is connected now.");
            clientConnectLatch.countDown();
            break;
        case Disconnected:
            LOG.info("ZooKeeper client is disconnected from zookeeper now,"
                + " but it is OK unless we received EXPIRED event.");
            break;
        case Expired:
            clientConnectLatch = new CountDownLatch(1);
            LOG.error("ZooKeeper client connection to the ZooKeeper server has expired!");
            break;
        default:
            // do nothing
            break;
        }
        // notify the child watchers
        notifyEvent(event);
    }

    /**
     * Waiting for the SyncConnected event from the ZooKeeper server.
     *
     * @throws KeeperException
     *             when there is no connection
     * @throws InterruptedException
     *             interrupted while waiting for connection
     */
    public void waitForConnection() throws KeeperException, InterruptedException {
        if (!clientConnectLatch.await(zkSessionTimeOut, TimeUnit.MILLISECONDS)) {
            throw KeeperException.create(KeeperException.Code.CONNECTIONLOSS);
        }
    }

    /**
     * Return zookeeper session time out.
     */
    public int getZkSessionTimeOut() {
        return zkSessionTimeOut;
    }

    /**
     * Notify Event to child watchers.
     *
     * @param event
     *          Watched event received from ZooKeeper.
     */
    private void notifyEvent(WatchedEvent event) {
        // notify child watchers
        for (Watcher w : childWatchers) {
            try {
                w.process(event);
            } catch (Exception t) {
                LOG.warn("Encountered unexpected exception from watcher {} : ", w, t);
            }
        }
    }

}
