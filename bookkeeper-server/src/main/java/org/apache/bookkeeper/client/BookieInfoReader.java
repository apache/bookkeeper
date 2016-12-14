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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.CollectionUtils;
import org.apache.bookkeeper.client.WeightedRandomSelection.WeightedObject;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetBookieInfoCallback;
import org.apache.bookkeeper.proto.BookkeeperProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BookieInfoReader {
    private static final Logger LOG = LoggerFactory.getLogger(BookieInfoReader.class);
    private final ScheduledExecutorService scheduler;
    private final BookKeeper bk;
    private final ClientConfiguration conf;
    private ConcurrentMap<BookieSocketAddress, BookieInfo> bookieInfoMap = new ConcurrentHashMap<BookieSocketAddress, BookieInfo>();
    private Collection<BookieSocketAddress> bookies;
    private final AtomicInteger totalSent = new AtomicInteger();
    private final AtomicInteger completedCnt = new AtomicInteger();
    private final AtomicBoolean instanceRunning = new AtomicBoolean();
    private final AtomicBoolean isQueued = new AtomicBoolean();
    private final AtomicBoolean refreshBookieList = new AtomicBoolean();

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
        public String toString() {
            return "FreeDiskSpace: " + this.freeDiskSpace + " TotalDiskCapacity: " + this.totalDiskSpace;
        }
    }

    BookieInfoReader(BookKeeper bk,
                          ClientConfiguration conf,
                          ScheduledExecutorService scheduler) {
        this.bk = bk;
        this.conf = conf;
        this.scheduler = scheduler;
    }
    void start() {
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                LOG.debug("Running periodic BookieInfo scan");
                getReadWriteBookieInfo(null);
            }
        }, 0, conf.getGetBookieInfoIntervalSeconds(), TimeUnit.SECONDS);
    }
    void submitTask(final Collection<BookieSocketAddress> newBookies) {
        scheduler.submit(new Runnable() {
            @Override
            public void run() {
                getReadWriteBookieInfo(newBookies);
            }
        });
    }
    void availableBookiesChanged(Set<BookieSocketAddress> newBookies) {
        LOG.info("Scheduling bookie info read due to changes in available bookies.");
        submitTask(newBookies);
    }

    /*
     * This routine is responsible for issuing bookieInfoGet messages to all the read write bookies.
     * instanceRunning will be true until we have sent the bookieInfoGet requests to
     * all the readwrite bookies and have processed all the callbacks. Only then is it reset to
     * false. At that time, if any pending tasks are queued, they are scheduled by the
     * last callback processing task. isQueued variable is used to indicate the pending
     * tasks. refreshBookieList is used to indicate that we need to read we need to explicitly
     * retireve the bookies list from zk because we don't remember the bookie list for
     * queued ops.
     */
    @SuppressWarnings("unchecked")
    void getReadWriteBookieInfo(Collection<BookieSocketAddress> newBookiesList) {
        if (instanceRunning.get() == false) {
            instanceRunning.compareAndSet(false, true);
        } else {
            isQueued.set(true);
            if (newBookiesList != null) {
                refreshBookieList.set(true);
            }
            LOG.debug("Exiting due to running instance");
            return;
        }
        Collection<BookieSocketAddress> deadBookies = null, joinedBookies=null;
        if (newBookiesList == null) {
            try {
                if (this.bookies == null) {
                    joinedBookies = this.bookies = bk.bookieWatcher.getBookies();
                } else if (refreshBookieList.get()) {
                    LOG.debug("Refreshing bookie list");
                    newBookiesList = bk.bookieWatcher.getBookies();
                    refreshBookieList.set(false);
                } else {
                    // the bookie list is already up to date, just retrieve their info
                    joinedBookies = this.bookies;
                }
            } catch (BKException e) {
                LOG.error("Unable to get the available bookies ", e);
                onExit();
                return;
            }
        }
        if (newBookiesList != null) {
            if (this.bookies != null) {
                joinedBookies = CollectionUtils.subtract(newBookiesList, this.bookies);
                deadBookies = CollectionUtils.subtract(this.bookies, newBookiesList);
                for (BookieSocketAddress b : deadBookies) {
                    bookieInfoMap.remove(b);
                    this.bookies.remove(b);
                }
                for (BookieSocketAddress b : joinedBookies) {
                    this.bookies.add(b);
                }
            } else {
                joinedBookies = this.bookies = newBookiesList;
            }
        }

        BookieClient bkc = bk.getBookieClient();
        final long requested = BookkeeperProtocol.GetBookieInfoRequest.Flags.TOTAL_DISK_CAPACITY_VALUE |
                               BookkeeperProtocol.GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE;
        totalSent.set(0);
        completedCnt.set(0);
        LOG.debug("Getting bookie info for: {}", joinedBookies);
        for (BookieSocketAddress b : joinedBookies) {
            bkc.getBookieInfo(b, requested,
                    new GetBookieInfoCallback() {
                        void processReadInfoComplete(int rc, BookieInfo bInfo, Object ctx) {
                            BookieSocketAddress b = (BookieSocketAddress) ctx;
                            if (rc != BKException.Code.OK) {
                                LOG.error("Reading bookie info from bookie {} failed due to error: {}.", b, rc);
                                // if there was data earlier, don't overwrite it
                                // create a new one only if the key was missing
                                bookieInfoMap.putIfAbsent(b, new BookieInfo());
                            } else {
                                LOG.debug("Bookie Info for bookie {} is {}", b, bInfo);
                                bookieInfoMap.put(b, bInfo);
                            }
                            if (completedCnt.incrementAndGet() == totalSent.get()) {
                                bk.placementPolicy.updateBookieInfo(bookieInfoMap);
                                onExit();
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
            totalSent.incrementAndGet();
        }
        if (totalSent.get() == 0) {
            if (deadBookies != null) {
                // if no new bookies joined but some existing bookies went away
                // we need to inform the placementPloicy
                bk.placementPolicy.updateBookieInfo(bookieInfoMap);
            }
            onExit();
        }
    }

    void onExit() {
        if (isQueued.get()) {
            LOG.debug("Scheduling a queued task");
            submitTask(null);
        }
        isQueued.set(false);
        instanceRunning.set(false);
    }

    Map<BookieSocketAddress, BookieInfo> getBookieInfo() throws BKException, InterruptedException {
        BookieClient bkc = bk.getBookieClient();
        final AtomicInteger totalSent = new AtomicInteger();
        final AtomicInteger totalCompleted = new AtomicInteger();
        final ConcurrentMap<BookieSocketAddress, BookieInfo> map = new ConcurrentHashMap<BookieSocketAddress, BookieInfo>();
        final CountDownLatch latch = new CountDownLatch(1);
        long requested = BookkeeperProtocol.GetBookieInfoRequest.Flags.TOTAL_DISK_CAPACITY_VALUE |
                         BookkeeperProtocol.GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE;

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
                                LOG.error("Reading bookie info from bookie {} failed due to error: {}.", b, rc);
                            } else {
                                LOG.debug("Free disk space on bookie {} is {}.", b, bInfo.getFreeDiskSpace());
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
            LOG.error("Received InterruptedException ", e);
            throw e;
        }
        return map;
    }
}
