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

import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.COOKIE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BKException.ZKException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.SafeRunnable;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.DataFormats.BookieServiceInfoFormat;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Version.Occurred;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * ZooKeeper based {@link RegistrationClient}.
 */
@Slf4j
public class ZKRegistrationClient implements RegistrationClient {

    static final int ZK_CONNECT_BACKOFF_MS = 200;

    class WatchTask
        implements SafeRunnable,
                   Watcher,
                   BiConsumer<Versioned<Set<BookieSocketAddress>>, Throwable>,
                   AutoCloseable {

        private final String regPath;
        private final Set<RegistrationListener> listeners;
        private volatile boolean closed = false;
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
                if (firstRunFuture.isDone()) {
                    scheduleWatchTask(ZK_CONNECT_BACKOFF_MS);
                } else {
                    firstRunFuture.completeExceptionally(throwable);
                }
                return;
            }

            if (this.version.compare(bookieSet.getVersion()) == Occurred.BEFORE) {
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

        boolean isClosed() {
            return closed;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private final ZooKeeper zk;
    private final ScheduledExecutorService scheduler;
    @Getter(AccessLevel.PACKAGE)
    private WatchTask watchWritableBookiesTask = null;
    @Getter(AccessLevel.PACKAGE)
    private WatchTask watchReadOnlyBookiesTask = null;

    // registration paths
    private final String bookieRegistrationPath;
    private final String bookieAllRegistrationPath;
    private final String bookieReadonlyRegistrationPath;

    public ZKRegistrationClient(ZooKeeper zk,
                                String ledgersRootPath,
                                ScheduledExecutorService scheduler) {
        this.zk = zk;
        this.scheduler = scheduler;

        this.bookieRegistrationPath = ledgersRootPath + "/" + AVAILABLE_NODE;
        this.bookieAllRegistrationPath = ledgersRootPath + "/" + COOKIE_NODE;
        this.bookieReadonlyRegistrationPath = this.bookieRegistrationPath + "/" + READONLY;
    }

    @Override
    public void close() {
        // no-op
    }

    public ZooKeeper getZk() {
        return zk;
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieSocketAddress>>> getWritableBookies() {
        return getChildren(bookieRegistrationPath, null);
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieSocketAddress>>> getAllBookies() {
        return getChildren(bookieAllRegistrationPath, null);
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieSocketAddress>>> getReadOnlyBookies() {
        return getChildren(bookieReadonlyRegistrationPath, null);
    }

    @Override
    public CompletableFuture<Versioned<BookieServiceInfo>> getBookieServiceInfo(String bookieId) {
        String pathAsWritable = bookieRegistrationPath + "/" + bookieId;
        String pathAsReadonly = bookieReadonlyRegistrationPath + "/" + bookieId;

        CompletableFuture<Versioned<BookieServiceInfo>> promise = new CompletableFuture<>();
        zk.getData(pathAsWritable, false, (int rc, String path, Object o, byte[] bytes, Stat stat) -> {
            if (KeeperException.Code.OK.intValue() == rc) {
                try {
                    BookieServiceInfo bookieServiceInfo = deserializeBookieServiceInfo(bookieId, bytes);
                    promise.complete(new Versioned<>(bookieServiceInfo, new LongVersion(stat.getCversion())));
                } catch (IOException ex) {
                    promise.completeExceptionally(KeeperException.create(KeeperException.Code.get(rc), path));
                    return;
                }
            } else if (KeeperException.Code.NONODE.intValue() == rc) {
                // not found, looking for a readonly bookie
                zk.getData(pathAsReadonly, false, (int rc2, String path2, Object o2, byte[] bytes2, Stat stat2) -> {
                    if (KeeperException.Code.OK.intValue() == rc2) {
                        try {
                            BookieServiceInfo bookieServiceInfo = deserializeBookieServiceInfo(bookieId, bytes2);
                            promise.complete(new Versioned<>(bookieServiceInfo, new LongVersion(stat2.getCversion())));
                        } catch (IOException ex) {
                            promise.completeExceptionally(KeeperException.create(KeeperException.Code.get(rc2), path2));
                            return;
                        }
                    } else {
                        // not found as writable and readonly, the bookie is offline
                        promise.completeExceptionally(BKException.create(BKException.Code.NoBookieAvailableException));
                    }
                }, null);
            } else {
                promise.completeExceptionally(KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }, null);
        return promise;
    }

    @SuppressWarnings("unchecked")
    @VisibleForTesting
    static BookieServiceInfo deserializeBookieServiceInfo(String bookieId, byte[] bookieServiceInfo)
            throws IOException {
        if (bookieServiceInfo == null || bookieServiceInfo.length == 0) {
            return BookieServiceInfoUtils.buildLegacyBookieServiceInfo(bookieId);
        }

        BookieServiceInfoFormat builder = BookieServiceInfoFormat.parseFrom(bookieServiceInfo);
        BookieServiceInfo bsi = new BookieServiceInfo();
        List<BookieServiceInfo.Endpoint> endpoints = builder.getEndpointsList().stream()
                .map(e -> {
                    BookieServiceInfo.Endpoint endpoint = new BookieServiceInfo.Endpoint();
                    endpoint.setId(e.getId());
                    endpoint.setPort(e.getPort());
                    endpoint.setHost(e.getHost());
                    endpoint.setProtocol(e.getProtocol());
                    endpoint.setAuth(e.getAuthList());
                    endpoint.setExtensions(e.getExtensionsList());
                    return endpoint;
                })
                .collect(Collectors.toList());

        bsi.setEndpoints(endpoints);
        bsi.setProperties(builder.getPropertiesMap());

        return bsi;
    }

    private CompletableFuture<Versioned<Set<BookieSocketAddress>>> getChildren(String regPath, Watcher watcher) {
        CompletableFuture<Versioned<Set<BookieSocketAddress>>> future = FutureUtils.createFuture();
        zk.getChildren(regPath, watcher, (rc, path, ctx, children, stat) -> {
            if (Code.OK != rc) {
                ZKException zke = new ZKException();
                future.completeExceptionally(zke.fillInStackTrace());
                return;
            }

            Version version = new LongVersion(stat.getCversion());
            Set<BookieSocketAddress> bookies = convertToBookieAddresses(children);
            future.complete(new Versioned<>(bookies, version));
        }, null);
        return future;
    }


    @Override
    public synchronized CompletableFuture<Void> watchWritableBookies(RegistrationListener listener) {
        CompletableFuture<Void> f;
        if (null == watchWritableBookiesTask) {
            f = new CompletableFuture<>();
            watchWritableBookiesTask = new WatchTask(bookieRegistrationPath, f);
            f = f.whenComplete((value, cause) -> {
                if (null != cause) {
                    unwatchWritableBookies(listener);
                }
            });
        } else {
            f = watchWritableBookiesTask.firstRunFuture;
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
        CompletableFuture<Void> f;
        if (null == watchReadOnlyBookiesTask) {
            f = new CompletableFuture<>();
            watchReadOnlyBookiesTask = new WatchTask(bookieReadonlyRegistrationPath, f);
            f = f.whenComplete((value, cause) -> {
                if (null != cause) {
                    unwatchReadOnlyBookies(listener);
                }
            });
        } else {
            f = watchReadOnlyBookiesTask.firstRunFuture;
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
