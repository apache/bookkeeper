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
package org.apache.distributedlog.bk;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.CustomLog;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.distributedlog.BookKeeperClient;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.exceptions.DLInterruptedException;
import org.apache.distributedlog.util.Transaction;
import org.apache.distributedlog.util.Utils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * LedgerAllocator impl.
 */
@CustomLog
public class LedgerAllocatorPool implements LedgerAllocator {

    private final DistributedLogConfiguration conf;
    private final QuorumConfigProvider quorumConfigProvider;
    private final BookKeeperClient bkc;
    private final ZooKeeperClient zkc;
    private final ScheduledExecutorService scheduledExecutorService;
    private final String poolPath;
    private final int corePoolSize;

    private final LinkedList<SimpleLedgerAllocator> pendingList =
            new LinkedList<SimpleLedgerAllocator>();
    private final LinkedList<SimpleLedgerAllocator> allocatingList =
            new LinkedList<SimpleLedgerAllocator>();
    private final Map<String, SimpleLedgerAllocator> rescueMap =
            new HashMap<String, SimpleLedgerAllocator>();
    private final Map<LedgerHandle, SimpleLedgerAllocator> obtainMap =
            new HashMap<LedgerHandle, SimpleLedgerAllocator>();
    private final Map<SimpleLedgerAllocator, LedgerHandle> reverseObtainMap =
            new HashMap<SimpleLedgerAllocator, LedgerHandle>();

    public LedgerAllocatorPool(String poolPath, int corePoolSize,
                               DistributedLogConfiguration conf,
                               ZooKeeperClient zkc,
                               BookKeeperClient bkc,
                               ScheduledExecutorService scheduledExecutorService) throws IOException {
        this.poolPath = poolPath;
        this.corePoolSize = corePoolSize;
        this.conf = conf;
        this.quorumConfigProvider =
                new ImmutableQuorumConfigProvider(conf.getQuorumConfig());
        this.zkc = zkc;
        this.bkc = bkc;
        this.scheduledExecutorService = scheduledExecutorService;
        initializePool();
    }

    @Override
    public void start() throws IOException {
        for (LedgerAllocator allocator : pendingList) {
            // issue allocating requests during initialize
            allocator.allocate();
        }
    }

    @VisibleForTesting
    synchronized int pendingListSize() {
        return pendingList.size();
    }

    @VisibleForTesting
    synchronized int allocatingListSize() {
        return allocatingList.size();
    }

    @VisibleForTesting
    public synchronized int obtainMapSize() {
        return obtainMap.size();
    }

    @VisibleForTesting
    synchronized int rescueSize() {
        return rescueMap.size();
    }

    @VisibleForTesting
    synchronized SimpleLedgerAllocator getLedgerAllocator(LedgerHandle lh) {
        return obtainMap.get(lh);
    }

    private void initializePool() throws IOException {
        try {
            List<String> allocators;
            try {
                allocators = zkc.get().getChildren(poolPath, false);
            } catch (KeeperException.NoNodeException e) {
                log.info().attr("poolPath", poolPath).log("Allocator Pool doesn't exist. Creating it.");
                ZkUtils.createFullPathOptimistic(zkc.get(), poolPath, new byte[0], zkc.getDefaultACL(),
                        CreateMode.PERSISTENT);
                allocators = zkc.get().getChildren(poolPath, false);
            }
            if (null == allocators) {
                allocators = new ArrayList<String>();
            }
            if (allocators.size() < corePoolSize) {
                createAllocators(corePoolSize - allocators.size());
                allocators = zkc.get().getChildren(poolPath, false);
            }
            initializeAllocators(allocators);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new DLInterruptedException("Interrupted when ensuring " + poolPath + " created : ", ie);
        } catch (KeeperException ke) {
            throw new IOException("Encountered zookeeper exception when initializing pool " + poolPath + " : ", ke);
        }
    }

    private void createAllocators(int numAllocators) throws InterruptedException, IOException {
        final AtomicInteger numPendings = new AtomicInteger(numAllocators);
        final AtomicInteger numFailures = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);
        AsyncCallback.StringCallback createCallback = new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                if (KeeperException.Code.OK.intValue() != rc) {
                    numFailures.incrementAndGet();
                    latch.countDown();
                    return;
                }
                if (numPendings.decrementAndGet() == 0 && numFailures.get() == 0) {
                    latch.countDown();
                }
            }
        };
        for (int i = 0; i < numAllocators; i++) {
            zkc.get().create(poolPath + "/A", new byte[0],
                             zkc.getDefaultACL(),
                             CreateMode.PERSISTENT_SEQUENTIAL,
                             createCallback, null);
        }
        latch.await();
        if (numFailures.get() > 0) {
            throw new IOException("Failed to create " + numAllocators + " allocators.");
        }
    }

    /**
     * Initialize simple allocators with given list of allocator names <i>allocators</i>.
     * It initializes a simple allocator with its simple allocator path.
     */
    private void initializeAllocators(List<String> allocators) throws IOException, InterruptedException {
        final AtomicInteger numPendings = new AtomicInteger(allocators.size());
        final AtomicInteger numFailures = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(numPendings.get() > 0 ? 1 : 0);
        AsyncCallback.DataCallback dataCallback = new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (KeeperException.Code.OK.intValue() != rc) {
                    numFailures.incrementAndGet();
                    latch.countDown();
                    return;
                }
                Versioned<byte[]> allocatorData =
                        new Versioned<byte[]>(data, new LongVersion(stat.getVersion()));
                SimpleLedgerAllocator allocator =
                        new SimpleLedgerAllocator(path, allocatorData, quorumConfigProvider, zkc, bkc);
                allocator.start();
                pendingList.add(allocator);
                if (numPendings.decrementAndGet() == 0 && numFailures.get() == 0) {
                    latch.countDown();
                }
            }
        };
        for (String name : allocators) {
            String path = poolPath + "/" + name;
            zkc.get().getData(path, false, dataCallback, null);
        }
        latch.await();
        if (numFailures.get() > 0) {
            throw new IOException("Failed to initialize allocators : " + allocators);
        }
    }

    private void scheduleAllocatorRescue(final SimpleLedgerAllocator ledgerAllocator) {
        try {
            scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        rescueAllocator(ledgerAllocator);
                    } catch (DLInterruptedException dle) {
                        Thread.currentThread().interrupt();
                    }
                }
            }, conf.getZKRetryBackoffStartMillis(), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ree) {
            log.warn()
                .attr("allocatePath", ledgerAllocator.allocatePath)
                .exception(ree)
                .log("Failed to schedule rescuing ledger allocator");
        }
    }

    /**
     * Rescue a ledger allocator from an ERROR state.
     * @param ledgerAllocator
     *          ledger allocator to rescue
     */
    private void rescueAllocator(final SimpleLedgerAllocator ledgerAllocator) throws DLInterruptedException {
        SimpleLedgerAllocator oldAllocator;
        synchronized (this) {
            oldAllocator = rescueMap.put(ledgerAllocator.allocatePath, ledgerAllocator);
        }
        if (oldAllocator != null) {
            log.info().attr("allocatePath", ledgerAllocator.allocatePath).log("ledger allocator is being rescued.");
            return;
        }
        try {
            zkc.get().getData(ledgerAllocator.allocatePath, false, new AsyncCallback.DataCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    boolean retry = false;
                    SimpleLedgerAllocator newAllocator = null;
                    if (KeeperException.Code.OK.intValue() == rc) {
                        Versioned<byte[]> allocatorData =
                                new Versioned<byte[]>(data, new LongVersion(stat.getVersion()));
                        log.info().attr("path", path).log("Rescuing ledger allocator.");
                        newAllocator = new SimpleLedgerAllocator(path, allocatorData, quorumConfigProvider, zkc, bkc);
                        newAllocator.start();
                        log.info().attr("path", path).log("Rescued ledger allocator.");
                    } else if (KeeperException.Code.NONODE.intValue() == rc) {
                        log.info().attr("path", path).log("Ledger allocator doesn't exist, skip rescuing it.");
                    } else {
                        retry = true;
                    }
                    synchronized (LedgerAllocatorPool.this) {
                        rescueMap.remove(ledgerAllocator.allocatePath);
                        if (null != newAllocator) {
                            pendingList.addLast(newAllocator);
                        }
                    }
                    if (retry) {
                        scheduleAllocatorRescue(ledgerAllocator);
                    }
                }
            }, null);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn()
                .attr("allocatePath", ledgerAllocator.allocatePath)
                .exception(ie)
                .log("Interrupted on rescuing ledger allocator");
            synchronized (LedgerAllocatorPool.this) {
                rescueMap.remove(ledgerAllocator.allocatePath);
            }
            throw new DLInterruptedException("Interrupted on rescuing ledger allocator "
                    + ledgerAllocator.allocatePath, ie);
        } catch (IOException ioe) {
            log.warn()
                .attr("allocatePath", ledgerAllocator.allocatePath)
                .exception(ioe)
                .log("Failed to rescue ledger allocator, retry rescuing it later");
            synchronized (LedgerAllocatorPool.this) {
                rescueMap.remove(ledgerAllocator.allocatePath);
            }
            scheduleAllocatorRescue(ledgerAllocator);
        }
    }

    @Override
    public void allocate() throws IOException {
        SimpleLedgerAllocator allocator;
        synchronized (this) {
            if (pendingList.isEmpty()) {
                // if no ledger allocator available, we should fail it immediately,
                // which the request will be redirected to other proxies
                throw new IOException("No ledger allocator available under " + poolPath + ".");
            } else {
                allocator = pendingList.removeFirst();
            }
        }
        boolean success = false;
        try {
            allocator.allocate();
            synchronized (this) {
                allocatingList.addLast(allocator);
            }
            success = true;
        } finally {
            if (!success) {
                rescueAllocator(allocator);
            }
        }
    }

    @Override
    public CompletableFuture<LedgerHandle> tryObtain(final Transaction<Object> txn,
                                                     final Transaction.OpListener<LedgerHandle> listener) {
        final SimpleLedgerAllocator allocator;
        synchronized (this) {
            if (allocatingList.isEmpty()) {
                return FutureUtils.exception(new IOException("No ledger allocator available under " + poolPath + "."));
            } else {
                allocator = allocatingList.removeFirst();
            }
        }

        final CompletableFuture<LedgerHandle> tryObtainPromise = new CompletableFuture<LedgerHandle>();
        final FutureEventListener<LedgerHandle> tryObtainListener = new FutureEventListener<LedgerHandle>() {
            @Override
            public void onSuccess(LedgerHandle lh) {
                synchronized (LedgerAllocatorPool.this) {
                    obtainMap.put(lh, allocator);
                    reverseObtainMap.put(allocator, lh);
                    tryObtainPromise.complete(lh);
                }
            }

            @Override
            public void onFailure(Throwable cause) {
                try {
                    rescueAllocator(allocator);
                } catch (IOException ioe) {
                    log.info()
                            .attr("allocatePath", allocator.allocatePath)
                            .exception(ioe)
                            .log("Failed to rescue allocator");
                }
                tryObtainPromise.completeExceptionally(cause);
            }
        };

        allocator.tryObtain(txn, new Transaction.OpListener<LedgerHandle>() {
            @Override
            public void onCommit(LedgerHandle lh) {
                confirmObtain(allocator);
                listener.onCommit(lh);
            }

            @Override
            public void onAbort(Throwable t) {
                abortObtain(allocator);
                listener.onAbort(t);
            }
        }).whenComplete(tryObtainListener);
        return tryObtainPromise;
    }

    void confirmObtain(SimpleLedgerAllocator allocator) {
        synchronized (this) {
            LedgerHandle lh = reverseObtainMap.remove(allocator);
            if (null != lh) {
                obtainMap.remove(lh);
            }
        }
        synchronized (this) {
            pendingList.addLast(allocator);
        }
    }

    void abortObtain(SimpleLedgerAllocator allocator) {
        synchronized (this) {
            LedgerHandle lh = reverseObtainMap.remove(allocator);
            if (null != lh) {
                obtainMap.remove(lh);
            }
        }
        // if a ledger allocator is aborted, it is better to rescue it. since the ledger allocator might
        // already encounter BadVersion exception.
        try {
            rescueAllocator(allocator);
        } catch (DLInterruptedException e) {
            log.warn().attr("poolPath", poolPath).exception(e).log("Interrupted on rescuing ledger allocator pool");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        List<LedgerAllocator> allocatorsToClose;
        synchronized (this) {
            allocatorsToClose = Lists.newArrayListWithExpectedSize(
                    pendingList.size() + allocatingList.size() + obtainMap.size());
            allocatorsToClose.addAll(pendingList);
            allocatorsToClose.addAll(allocatingList);
            allocatorsToClose.addAll(obtainMap.values());
        }
        return FutureUtils.processList(
            allocatorsToClose,
            allocator -> allocator.asyncClose(),
            scheduledExecutorService
        ).thenApply(values -> null);
    }

    @Override
    public CompletableFuture<Void> delete() {
        List<LedgerAllocator> allocatorsToDelete;
        synchronized (this) {
            allocatorsToDelete = Lists.newArrayListWithExpectedSize(
                    pendingList.size() + allocatingList.size() + obtainMap.size());
            allocatorsToDelete.addAll(pendingList);
            allocatorsToDelete.addAll(allocatingList);
            allocatorsToDelete.addAll(obtainMap.values());
        }
        return FutureUtils.processList(
            allocatorsToDelete,
            allocator -> allocator.delete(),
            scheduledExecutorService
        ).thenCompose(values -> Utils.zkDelete(zkc, poolPath, new LongVersion(-1)));
    }
}
