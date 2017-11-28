/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.discover;

import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKInterruptedException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BKException.ZKException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.SafeRunnable;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Version.Occurred;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * ZooKeeper based {@link RegistrationClient}.
 */
@Slf4j
public class ZKRegistrationClient implements RegistrationClient {

    private static final int ZK_CONNECT_BACKOFF_MS = 200;

    private class WatchTask
        implements SafeRunnable,
                   Watcher,
                   BiConsumer<Versioned<Set<BookieSocketAddress>>, Throwable>,
                   AutoCloseable {

        private final String regPath;
        private final Set<RegistrationListener> listeners;
        private boolean closed = false;
        private Set<BookieSocketAddress> bookies = null;
        private Version version = Version.NEW;
        private final CompletableFuture<Void> firstRunFuture;

        WatchTask(String regPath, CompletableFuture<Void> firstRunFuture) {
            this.regPath = regPath;
            this.listeners = new CopyOnWriteArraySet<>();
            this.firstRunFuture = firstRunFuture;
        }

        public int getNumListeners() {
            return listeners.size();
        }

        public boolean addListener(RegistrationListener listener) {
            if (listeners.add(listener)) {
                if (null != bookies) {
                    scheduler.execute(() -> {
                            listener.onBookiesChanged(
                                    new Versioned<>(bookies, version));
                        });
                }
            }
            return true;
        }

        public boolean removeListener(RegistrationListener listener) {
            return listeners.remove(listener);
        }

        void watch() {
            scheduleWatchTask(0L);
        }

        private void scheduleWatchTask(long delayMs) {
            try {
                scheduler.schedule(this, delayMs, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException ree) {
                log.warn("Failed to schedule watch bookies task", ree);
            }
        }

        @Override
        public void safeRun() {
            if (isClosed()) {
                return;
            }

            getChildren(regPath, this)
                .whenCompleteAsync(this, scheduler);
        }

        @Override
        public void accept(Versioned<Set<BookieSocketAddress>> bookieSet, Throwable throwable) {
            if (throwable != null) {
                scheduleWatchTask(ZK_CONNECT_BACKOFF_MS);
                firstRunFuture.completeExceptionally(throwable);
                return;
            }

            if (this.version.compare(bookieSet.getVersion()) == Occurred.BEFORE
                || this.version.compare(bookieSet.getVersion()) == Occurred.CONCURRENTLY) {
                this.version = bookieSet.getVersion();
                this.bookies = bookieSet.getValue();

                for (RegistrationListener listener : listeners) {
                    listener.onBookiesChanged(bookieSet);
                }
            }
            FutureUtils.complete(firstRunFuture, null);
        }

        @Override
        public void process(WatchedEvent event) {
            if (EventType.None == event.getType()) {
                if (KeeperState.Expired == event.getState()) {
                    scheduleWatchTask(ZK_CONNECT_BACKOFF_MS);
                }
                return;
            }

            // re-read the bookie list
            scheduleWatchTask(0L);
        }

        synchronized boolean isClosed() {
            return closed;
        }

        @Override
        public synchronized void close() {
            if (!closed) {
                return;
            }
            closed = true;
        }
    }

    private ClientConfiguration conf;
    private ZooKeeper zk = null;
    // whether the zk handle is one we created, or is owned by whoever
    // instantiated us
    private boolean ownZKHandle = false;
    private ScheduledExecutorService scheduler;
    private WatchTask watchWritableBookiesTask = null;
    private WatchTask watchReadOnlyBookiesTask = null;

    // registration paths
    private String bookieRegistrationPath;
    private String bookieReadonlyRegistrationPath;

    @Override
    public RegistrationClient initialize(ClientConfiguration conf,
                                         ScheduledExecutorService scheduler,
                                         StatsLogger statsLogger,
                                         Optional<ZooKeeper> zkOptional)
            throws BKException {
        this.conf = conf;
        this.scheduler = scheduler;

        this.bookieRegistrationPath = conf.getZkAvailableBookiesPath();
        this.bookieReadonlyRegistrationPath = this.bookieRegistrationPath + "/" + READONLY;

        // construct the zookeeper
        if (zkOptional.isPresent()) {
            this.zk = zkOptional.get();
            this.ownZKHandle = false;
        } else {
            try {
                this.zk = ZooKeeperClient.newBuilder()
                    .connectString(conf.getZkServers())
                    .sessionTimeoutMs(conf.getZkTimeout())
                    .operationRetryPolicy(new BoundExponentialBackoffRetryPolicy(conf.getZkTimeout(),
                        conf.getZkTimeout(), 0))
                    .statsLogger(statsLogger)
                    .build();

                if (null == zk.exists(bookieReadonlyRegistrationPath, false)) {
                    try {
                        List<ACL> zkAcls = ZkUtils.getACLs(conf);
                        zk.create(bookieReadonlyRegistrationPath,
                            new byte[0],
                            zkAcls,
                            CreateMode.PERSISTENT);
                    } catch (KeeperException.NodeExistsException e) {
                        // this node is just now created by someone.
                    }
                }
            } catch (IOException | KeeperException e) {
                log.error("Failed to create zookeeper client to {}", conf.getZkServers(), e);
                ZKException zke = new ZKException();
                zke.fillInStackTrace();
                throw zke;
            } catch (InterruptedException e) {
                throw new BKInterruptedException();
            }
            this.ownZKHandle = true;
        }

        return this;
    }

    @Override
    public void close() {
        if (ownZKHandle && null != zk) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                log.warn("Interrupted on closing zookeeper client", e);
            }
        }
    }

    public ZooKeeper getZk() {
        return zk;
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieSocketAddress>>> getWritableBookies() {
        return getChildren(bookieRegistrationPath, null);
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieSocketAddress>>> getReadOnlyBookies() {
        return getChildren(bookieReadonlyRegistrationPath, null);
    }

    private CompletableFuture<Versioned<Set<BookieSocketAddress>>> getChildren(String regPath, Watcher watcher) {
        CompletableFuture<Versioned<Set<BookieSocketAddress>>> future = FutureUtils.createFuture();
        zk.getChildren(regPath, watcher, (rc, path, ctx, children, stat) -> {
            if (Code.OK != rc) {
                ZKException zke = new ZKException();
                zke.fillInStackTrace();
                future.completeExceptionally(zke);
                return;
            }

            Version version = new LongVersion(stat.getVersion());
            Set<BookieSocketAddress> bookies = convertToBookieAddresses(children);
            future.complete(new Versioned<>(bookies, version));
        }, null);
        return future;
    }


    @Override
    public synchronized CompletableFuture<Void> watchWritableBookies(RegistrationListener listener) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        if (null == watchWritableBookiesTask) {
            watchWritableBookiesTask = new WatchTask(bookieRegistrationPath, f);
        }

        watchWritableBookiesTask.addListener(listener);
        if (watchWritableBookiesTask.getNumListeners() == 1) {
            watchWritableBookiesTask.watch();
        }
        return f;
    }

    @Override
    public synchronized void unwatchWritableBookies(RegistrationListener listener) {
        if (null == watchWritableBookiesTask) {
            return;
        }

        watchWritableBookiesTask.removeListener(listener);
        if (watchWritableBookiesTask.getNumListeners() == 0) {
            watchWritableBookiesTask.close();
            watchWritableBookiesTask = null;
        }
    }

    @Override
    public synchronized CompletableFuture<Void> watchReadOnlyBookies(RegistrationListener listener) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        if (null == watchReadOnlyBookiesTask) {
            watchReadOnlyBookiesTask = new WatchTask(bookieReadonlyRegistrationPath, f);
        }

        watchReadOnlyBookiesTask.addListener(listener);
        if (watchReadOnlyBookiesTask.getNumListeners() == 1) {
            watchReadOnlyBookiesTask.watch();
        }
        return f;
    }

    @Override
    public synchronized void unwatchReadOnlyBookies(RegistrationListener listener) {
        if (null == watchReadOnlyBookiesTask) {
            return;
        }

        watchReadOnlyBookiesTask.removeListener(listener);
        if (watchReadOnlyBookiesTask.getNumListeners() == 0) {
            watchReadOnlyBookiesTask.close();
            watchReadOnlyBookiesTask = null;
        }
    }

    private static HashSet<BookieSocketAddress> convertToBookieAddresses(List<String> children) {
        // Read the bookie addresses into a set for efficient lookup
        HashSet<BookieSocketAddress> newBookieAddrs = Sets.newHashSet();
        for (String bookieAddrString : children) {
            if (READONLY.equals(bookieAddrString)) {
                continue;
            }

            BookieSocketAddress bookieAddr;
            try {
                bookieAddr = new BookieSocketAddress(bookieAddrString);
            } catch (IOException e) {
                log.error("Could not parse bookie address: " + bookieAddrString + ", ignoring this bookie");
                continue;
            }
            newBookieAddrs.add(bookieAddr);
        }
        return newBookieAddrs;
    }

}
