/**
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
package org.apache.bookkeeper.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.WeightedRandomSelection.WeightedObject;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetBookieInfoCallback;
import org.apache.bookkeeper.proto.BookkeeperProtocol;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class to read {@link BookieInfo} from bookies.
 *
 * <p>NOTE: This class is tended to be used by this project only. External users should not rely on it directly.
 */
public class BookieInfoReader {
    private static final Logger LOG = LoggerFactory.getLogger(BookieInfoReader.class);
    private static final long GET_BOOKIE_INFO_REQUEST_FLAGS =
        BookkeeperProtocol.GetBookieInfoRequest.Flags.TOTAL_DISK_CAPACITY_VALUE
                               | BookkeeperProtocol.GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE;

    private final ScheduledExecutorService scheduler;
    private final BookKeeper bk;
    private final ClientConfiguration conf;

    /**
     * A class represents the information (e.g. disk usage, load) of a bookie.
     *
     * <p>NOTE: This class is tended to be used by this project only. External users should not rely on it directly.
     */
    public static class BookieInfo implements WeightedObject {
        private final long freeDiskSpace;
        private final long totalDiskSpace;
        public BookieInfo() {
            this(0L, 0L);
        }
        public BookieInfo(long totalDiskSpace, long freeDiskSpace) {
            this.totalDiskSpace = totalDiskSpace;
            this.freeDiskSpace = freeDiskSpace;
        }
        public long getFreeDiskSpace() {
            return freeDiskSpace;
        }
        public long getTotalDiskSpace() {
            return totalDiskSpace;
        }
        @Override
        public long getWeight() {
            return freeDiskSpace;
        }
        @Override
        public String toString() {
            return "FreeDiskSpace: " + this.freeDiskSpace + " TotalDiskCapacity: " + this.totalDiskSpace;
        }
    }


    /**
     * Tracks the most recently reported set of bookies from BookieWatcher as well
     * as current BookieInfo for bookies we've successfully queried.
     */
    private static class BookieInfoMap {
        /**
         * Contains the most recently obtained information on the contained bookies.
         * When an error happens querying a bookie, the entry is removed.
         */
        private final Map<BookieSocketAddress, BookieInfo> infoMap = new HashMap<>();

        /**
         * Contains the most recently reported set of bookies from BookieWatcher
         * A partial query consists of every member of mostRecentlyReportedBookies
         * minus the entries in bookieInfoMap.
         */
        private Collection<BookieSocketAddress> mostRecentlyReportedBookies = new ArrayList<>();

        public void updateBookies(Collection<BookieSocketAddress> updatedBookieSet) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "updateBookies: current: {}, new: {}",
                        mostRecentlyReportedBookies, updatedBookieSet);
            }
            infoMap.keySet().retainAll(updatedBookieSet);
            mostRecentlyReportedBookies = updatedBookieSet;
        }

        @SuppressWarnings("unchecked")
        public Collection<BookieSocketAddress> getPartialScanTargets() {
            return CollectionUtils.subtract(mostRecentlyReportedBookies, infoMap.keySet());
        }

        public Collection<BookieSocketAddress> getFullScanTargets() {
            return mostRecentlyReportedBookies;
        }

        /**
         * Returns info for bookie, null if not known.
         *
         * @param bookie bookie for which to get info
         * @return Info for bookie, null otherwise
         */
        public BookieInfo getInfo(BookieSocketAddress bookie) {
            return infoMap.get(bookie);
        }

        /**
         * Removes bookie from bookieInfoMap.
         *
         * @param bookie bookie on which we observed an error
         */
        public void clearInfo(BookieSocketAddress bookie) {
            infoMap.remove(bookie);
        }

        /**
         * Report new info on bookie.
         *
         * @param bookie bookie for which we obtained new info
         * @param info the new info
         */
        public void gotInfo(BookieSocketAddress bookie, BookieInfo info) {
            infoMap.put(bookie, info);
        }

        /**
         * Get bookie info map.
         */
        public Map<BookieSocketAddress, BookieInfo> getBookieMap() {
            return infoMap;
        }
    }
    private final BookieInfoMap bookieInfoMap = new BookieInfoMap();

    /**
     * Tracks whether there is an execution in progress as well as whether
     * another is pending.
     */
    public enum State { UNQUEUED, PARTIAL, FULL }
    private static class InstanceState {
        private boolean running = false;
        private State queuedType = State.UNQUEUED;

        private boolean shouldStart() {
            if (running) {
                return false;
            } else {
                running = true;
                return true;
            }
        }

        /**
         * Mark pending operation FULL and return true if there is no in-progress operation.
         *
         * @return True if we should execute a scan, False if there is already one running
         */
        public boolean tryStartFull() {
            queuedType = State.FULL;
            return shouldStart();
        }

        /**
         * Mark pending operation PARTIAL if not full and return true if there is no in-progress operation.
         *
         * @return True if we should execute a scan, False if there is already one running
         */
        public boolean tryStartPartial() {
            if (queuedType == State.UNQUEUED) {
                queuedType = State.PARTIAL;
            }
            return shouldStart();
        }

        /**
         * Gets and clears queuedType.
         */
        public State getAndClearQueuedType() {
            State ret = queuedType;
            queuedType = State.UNQUEUED;
            return ret;
        }

        /**
         * If queuedType != UNQUEUED, returns true, leaves running equal to true
         * Otherwise, returns false and sets running to false.
         */
        public boolean completeUnlessQueued() {
            if (queuedType == State.UNQUEUED) {
                running = false;
                return false;
            } else {
                return true;
            }
        }
    }
    private final InstanceState instanceState = new InstanceState();

    BookieInfoReader(BookKeeper bk,
                     ClientConfiguration conf,
                     ScheduledExecutorService scheduler) {
        this.bk = bk;
        this.conf = conf;
        this.scheduler = scheduler;
    }

    public void start() {
        this.bk
            .getMetadataClientDriver()
            .getRegistrationClient()
            .watchWritableBookies(bookies -> availableBookiesChanged(bookies.getValue()));
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                synchronized (BookieInfoReader.this) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Running periodic BookieInfo scan");
                    }
                    try {
                        Collection<BookieSocketAddress> updatedBookies = bk.bookieWatcher.getBookies();
                        bookieInfoMap.updateBookies(updatedBookies);
                    } catch (BKException e) {
                        LOG.info("Got exception while querying bookies from watcher, rerunning after {}s",
                                 conf.getGetBookieInfoRetryIntervalSeconds(), e);
                        scheduler.schedule(this, conf.getGetBookieInfoRetryIntervalSeconds(), TimeUnit.SECONDS);
                        return;
                    }
                    if (instanceState.tryStartFull()) {
                        getReadWriteBookieInfo();
                    }
                }
            }
        }, 0, conf.getGetBookieInfoIntervalSeconds(), TimeUnit.SECONDS);
    }

    private void submitTask() {
        scheduler.submit(() -> getReadWriteBookieInfo());
    }

    private void submitTaskWithDelay(int delaySeconds) {
        scheduler.schedule(() -> getReadWriteBookieInfo(), delaySeconds, TimeUnit.SECONDS);
    }

    synchronized void availableBookiesChanged(Set<BookieSocketAddress> updatedBookiesList) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Scheduling bookie info read due to changes in available bookies.");
        }
        bookieInfoMap.updateBookies(updatedBookiesList);
        if (instanceState.tryStartPartial()) {
            submitTask();
        }
    }

    /**
     * Method to allow tests to block until bookie info is available.
     *
     * @param bookie to lookup
     * @return None if absent, free disk space if present
     */
    synchronized Optional<Long> getFreeDiskSpace(BookieSocketAddress bookie) {
        BookieInfo bookieInfo = bookieInfoMap.getInfo(bookie);
        if (bookieInfo != null) {
            return Optional.of(bookieInfo.getFreeDiskSpace());
        } else {
            return Optional.empty();
        }
    }

    /* State to track scan execution progress as callbacks come in */
    private int totalSent = 0;
    private int completedCnt = 0;
    private int errorCnt = 0;

    /**
     * Performs scan described by instanceState using the cached bookie information
     * in bookieInfoMap.
     */
    synchronized void getReadWriteBookieInfo() {
        State queuedType = instanceState.getAndClearQueuedType();
        Collection<BookieSocketAddress> toScan;
        if (queuedType == State.FULL) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Doing full scan");
            }
            toScan = bookieInfoMap.getFullScanTargets();
        } else if (queuedType == State.PARTIAL) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Doing partial scan");
            }
            toScan = bookieInfoMap.getPartialScanTargets();
        } else {
            if (LOG.isErrorEnabled()) {
                LOG.error("Invalid state, queuedType cannot be UNQUEUED in getReadWriteBookieInfo");
            }
            assert(queuedType != State.UNQUEUED);
            return;
        }

        BookieClient bkc = bk.getBookieClient();
        final long requested = BookkeeperProtocol.GetBookieInfoRequest.Flags.TOTAL_DISK_CAPACITY_VALUE
                               | BookkeeperProtocol.GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE;
        totalSent = 0;
        completedCnt = 0;
        errorCnt = 0;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting bookie info for: {}", toScan);
        }
        for (BookieSocketAddress b : toScan) {
            bkc.getBookieInfo(b, requested,
                    new GetBookieInfoCallback() {
                        void processReadInfoComplete(int rc, BookieInfo bInfo, Object ctx) {
                            synchronized (BookieInfoReader.this) {
                                BookieSocketAddress b = (BookieSocketAddress) ctx;
                                if (rc != BKException.Code.OK) {
                                    if (LOG.isErrorEnabled()) {
                                        LOG.error("Reading bookie info from bookie {} failed due to {}",
                                                b, BKException.codeLogger(rc));
                                    }
                                    // We reread bookies missing from the map each time, so remove to ensure
                                    // we get to it on the next scan
                                    bookieInfoMap.clearInfo(b);
                                    errorCnt++;
                                } else {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("Bookie Info for bookie {} is {}", b, bInfo);
                                    }
                                    bookieInfoMap.gotInfo(b, bInfo);
                                }
                                completedCnt++;
                                if (totalSent == completedCnt) {
                                    onExit();
                                }
                            }
                        }
                        @Override
                        public void getBookieInfoComplete(final int rc, final BookieInfo bInfo, final Object ctx) {
                            scheduler.submit(
                                new Runnable() {
                                    @Override
                                    public void run() {
                                        processReadInfoComplete(rc, bInfo, ctx);
                                    }
                                });
                        }
                    }, b);
            totalSent++;
        }
        if (totalSent == 0) {
            onExit();
        }
    }

    void onExit() {
        bk.placementPolicy.updateBookieInfo(bookieInfoMap.getBookieMap());
        if (errorCnt > 0) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Rescheduling in {}s due to errors", conf.getGetBookieInfoIntervalSeconds());
            }
            instanceState.tryStartPartial();
            submitTaskWithDelay(conf.getGetBookieInfoRetryIntervalSeconds());
        } else if (instanceState.completeUnlessQueued()) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Rescheduling, another scan is pending");
            }
            submitTask();
        }
    }

    Map<BookieSocketAddress, BookieInfo> getBookieInfo() throws BKException, InterruptedException {
        BookieClient bkc = bk.getBookieClient();
        final AtomicInteger totalSent = new AtomicInteger();
        final AtomicInteger totalCompleted = new AtomicInteger();
        final ConcurrentMap<BookieSocketAddress, BookieInfo> map =
            new ConcurrentHashMap<BookieSocketAddress, BookieInfo>();
        final CountDownLatch latch = new CountDownLatch(1);
        long requested = BookkeeperProtocol.GetBookieInfoRequest.Flags.TOTAL_DISK_CAPACITY_VALUE
                         | BookkeeperProtocol.GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE;

        Collection<BookieSocketAddress> bookies;
        bookies = bk.bookieWatcher.getBookies();
        bookies.addAll(bk.bookieWatcher.getReadOnlyBookies());

        totalSent.set(bookies.size());
        for (BookieSocketAddress b : bookies) {
            bkc.getBookieInfo(b, requested, new GetBookieInfoCallback() {
                        @Override
                        public void getBookieInfoComplete(int rc, BookieInfo bInfo, Object ctx) {
                            BookieSocketAddress b = (BookieSocketAddress) ctx;
                            if (rc != BKException.Code.OK) {
                                if (LOG.isErrorEnabled()) {
                                    LOG.error("Reading bookie info from bookie {} failed due to {}",
                                            b, BKException.codeLogger(rc));
                                }
                            } else {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Free disk space on bookie {} is {}.", b, bInfo.getFreeDiskSpace());
                                }
                                map.put(b, bInfo);
                            }
                            if (totalCompleted.incrementAndGet() == totalSent.get()) {
                                latch.countDown();
                            }
                        }
                    }, b);
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Received InterruptedException ", e);
            throw e;
        }
        return map;
    }
}
