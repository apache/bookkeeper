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
package org.apache.distributedlog.auditor;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAccessor;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.distributedlog.BookKeeperClient;
import org.apache.distributedlog.BookKeeperClientBuilder;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.LogSegmentMetadata;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.ZooKeeperClientBuilder;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.exceptions.DLInterruptedException;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.impl.BKNamespaceDriver;
import org.apache.distributedlog.impl.metadata.BKDLConfig;
import org.apache.distributedlog.namespace.NamespaceDriver;
import org.apache.distributedlog.util.DLUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




/**
 * DL Auditor will audit DL namespace, e.g. find leaked ledger, report disk usage by streams.
 */
public class DLAuditor {

    private static final Logger logger = LoggerFactory.getLogger(DLAuditor.class);

    private final DistributedLogConfiguration conf;

    public DLAuditor(DistributedLogConfiguration conf) {
        this.conf = conf;
    }

    private ZooKeeperClient getZooKeeperClient(Namespace namespace) {
        NamespaceDriver driver = namespace.getNamespaceDriver();
        assert(driver instanceof BKNamespaceDriver);
        return ((BKNamespaceDriver) driver).getWriterZKC();
    }

    private BookKeeperClient getBookKeeperClient(Namespace namespace) {
        NamespaceDriver driver = namespace.getNamespaceDriver();
        assert(driver instanceof BKNamespaceDriver);
        return ((BKNamespaceDriver) driver).getReaderBKC();
    }

    private String validateAndGetZKServers(List<URI> uris) {
        URI firstURI = uris.get(0);
        String zkServers = BKNamespaceDriver.getZKServersFromDLUri(firstURI);
        for (URI uri : uris) {
            if (!zkServers.equalsIgnoreCase(BKNamespaceDriver.getZKServersFromDLUri(uri))) {
                throw new IllegalArgumentException("Uris don't belong to same zookeeper cluster");
            }
        }
        return zkServers;
    }

    private BKDLConfig resolveBKDLConfig(ZooKeeperClient zkc, List<URI> uris) throws IOException {
        URI firstURI = uris.get(0);
        BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(zkc, firstURI);
        for (URI uri : uris) {
            BKDLConfig anotherConfig = BKDLConfig.resolveDLConfig(zkc, uri);
            if (!(Objects.equal(bkdlConfig.getBkLedgersPath(), anotherConfig.getBkLedgersPath())
                    && Objects.equal(bkdlConfig.getBkZkServersForWriter(), anotherConfig.getBkZkServersForWriter()))) {
                throw new IllegalArgumentException("Uris don't use same bookkeeper cluster");
            }
        }
        return bkdlConfig;
    }

    public Pair<Set<Long>, Set<Long>> collectLedgers(List<URI> uris, List<List<String>> allocationPaths)
            throws IOException {
        checkArgument(uris.size() > 0, "No uri provided to audit");

        String zkServers = validateAndGetZKServers(uris);
        RetryPolicy retryPolicy = new BoundExponentialBackoffRetryPolicy(
                conf.getZKRetryBackoffStartMillis(),
                conf.getZKRetryBackoffMaxMillis(),
                Integer.MAX_VALUE);
        ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder()
                .name("DLAuditor-ZK")
                .zkServers(zkServers)
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .retryPolicy(retryPolicy)
                .zkAclId(conf.getZkAclId())
                .build();
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            BKDLConfig bkdlConfig = resolveBKDLConfig(zkc, uris);
            logger.info("Resolved bookkeeper config : {}", bkdlConfig);

            BookKeeperClient bkc = BookKeeperClientBuilder.newBuilder()
                    .name("DLAuditor-BK")
                    .dlConfig(conf)
                    .zkServers(bkdlConfig.getBkZkServersForWriter())
                    .ledgersPath(bkdlConfig.getBkLedgersPath())
                    .build();
            try {
                Set<Long> bkLedgers = collectLedgersFromBK(bkc, executorService);
                Set<Long> dlLedgers = collectLedgersFromDL(uris, allocationPaths);
                return Pair.of(bkLedgers, dlLedgers);
            } finally {
                bkc.close();
            }
        } finally {
            zkc.close();
            executorService.shutdown();
        }
    }

    /**
     * Find leak ledgers phase 1: collect ledgers set.
     */
    private Set<Long> collectLedgersFromBK(BookKeeperClient bkc,
                                           final ExecutorService executorService)
            throws IOException {
        LedgerManager lm = BookKeeperAccessor.getLedgerManager(bkc.get());

        final Set<Long> ledgers = new HashSet<Long>();
        final CompletableFuture<Void> doneFuture = FutureUtils.createFuture();

        BookkeeperInternalCallbacks.Processor<Long> collector =
                new BookkeeperInternalCallbacks.Processor<Long>() {
            @Override
            public void process(Long lid,
                                final AsyncCallback.VoidCallback cb) {
                synchronized (ledgers) {
                    ledgers.add(lid);
                    if (0 == ledgers.size() % 1000) {
                        logger.info("Collected {} ledgers", ledgers.size());
                    }
                }
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        cb.processResult(BKException.Code.OK, null, null);
                    }
                });

            }
        };
        AsyncCallback.VoidCallback finalCb = new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (BKException.Code.OK == rc) {
                    doneFuture.complete(null);
                } else {
                    doneFuture.completeExceptionally(BKException.create(rc));
                }
            }
        };
        lm.asyncProcessLedgers(collector, finalCb, null, BKException.Code.OK,
                BKException.Code.ZKException);
        try {
            doneFuture.get();
            logger.info("Collected total {} ledgers", ledgers.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DLInterruptedException("Interrupted on collecting ledgers : ", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) (e.getCause());
            } else {
                throw new IOException("Failed to collect ledgers : ", e.getCause());
            }
        }
        return ledgers;
    }

    /**
     * Find leak ledgers phase 2: collect ledgers from uris.
     */
    private Set<Long> collectLedgersFromDL(List<URI> uris, List<List<String>> allocationPaths)
            throws IOException {
        final Set<Long> ledgers = new TreeSet<Long>();
        List<Namespace> namespaces =
                new ArrayList<Namespace>(uris.size());
        try {
            for (URI uri : uris) {
                namespaces.add(
                        NamespaceBuilder.newBuilder()
                                .conf(conf)
                                .uri(uri)
                                .build());
            }
            final CountDownLatch doneLatch = new CountDownLatch(uris.size());
            final AtomicInteger numFailures = new AtomicInteger(0);
            ExecutorService executor = Executors.newFixedThreadPool(uris.size());
            try {
                int i = 0;
                for (final Namespace namespace : namespaces) {
                    final Namespace dlNamespace = namespace;
                    final URI uri = uris.get(i);
                    final List<String> aps = allocationPaths.get(i);
                    i++;
                    executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                logger.info("Collecting ledgers from {} : {}", uri, aps);
                                collectLedgersFromAllocator(uri, namespace, aps, ledgers);
                                synchronized (ledgers) {
                                    logger.info("Collected {} ledgers from allocators for {} : {} ",
                                        ledgers.size(), uri, ledgers);
                                }
                                collectLedgersFromDL(uri, namespace, ledgers);
                            } catch (IOException e) {
                                numFailures.incrementAndGet();
                                logger.info("Error to collect ledgers from DL : ", e);
                            }
                            doneLatch.countDown();
                        }
                    });
                }
                try {
                    doneLatch.await();
                    if (numFailures.get() > 0) {
                        throw new IOException(numFailures.get() + " errors to collect ledgers from DL");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Interrupted on collecting ledgers from DL : ", e);
                    throw new DLInterruptedException("Interrupted on collecting ledgers from DL : ", e);
                }
            } finally {
                executor.shutdown();
            }
        } finally {
            for (Namespace namespace : namespaces) {
                namespace.close();
            }
        }
        return ledgers;
    }

    private void collectLedgersFromAllocator(final URI uri,
                                             final Namespace namespace,
                                             final List<String> allocationPaths,
                                             final Set<Long> ledgers) throws IOException {
        final LinkedBlockingQueue<String> poolQueue =
                new LinkedBlockingQueue<String>();
        for (String allocationPath : allocationPaths) {
            String rootPath = uri.getPath() + "/" + allocationPath;
            try {
                List<String> pools = getZooKeeperClient(namespace).get().getChildren(rootPath, false);
                for (String pool : pools) {
                    poolQueue.add(rootPath + "/" + pool);
                }
            } catch (KeeperException e) {
                throw new ZKException("Failed to get list of pools from " + rootPath, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DLInterruptedException("Interrupted on getting list of pools from " + rootPath, e);
            }
        }


        logger.info("Collecting ledgers from allocators for {} : {}", uri, poolQueue);

        executeAction(poolQueue, 10, new Action<String>() {
            @Override
            public void execute(String poolPath) throws IOException {
                try {
                    collectLedgersFromPool(poolPath);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new DLInterruptedException("Interrupted on collecting"
                            + " ledgers from allocation pool " + poolPath, e);
                } catch (KeeperException e) {
                    throw new ZKException("Failed to collect ledgers from allocation pool " + poolPath, e.code());
                }
            }

            private void collectLedgersFromPool(String poolPath)
                    throws InterruptedException, ZooKeeperClient.ZooKeeperConnectionException, KeeperException {
                List<String> allocators = getZooKeeperClient(namespace).get()
                                        .getChildren(poolPath, false);
                for (String allocator : allocators) {
                    String allocatorPath = poolPath + "/" + allocator;
                    byte[] data = getZooKeeperClient(namespace).get().getData(allocatorPath, false, new Stat());
                    if (null != data && data.length > 0) {
                        try {
                            long ledgerId = DLUtils.bytes2LogSegmentId(data);
                            synchronized (ledgers) {
                                ledgers.add(ledgerId);
                            }
                        } catch (NumberFormatException nfe) {
                            logger.warn("Invalid ledger found in allocator path {} : ", allocatorPath, nfe);
                        }
                    }
                }
            }
        });

        logger.info("Collected ledgers from allocators for {}.", uri);
    }

    private void collectLedgersFromDL(final URI uri,
                                      final Namespace namespace,
                                      final Set<Long> ledgers) throws IOException {
        logger.info("Enumerating {} to collect streams.", uri);
        Iterator<String> streams = namespace.getLogs();
        final LinkedBlockingQueue<String> streamQueue = new LinkedBlockingQueue<String>();
        while (streams.hasNext()) {
            streamQueue.add(streams.next());
        }

        logger.info("Collected {} streams from uri {} : {}", streamQueue.size(), uri, streams);

        executeAction(streamQueue, 10, new Action<String>() {
            @Override
            public void execute(String stream) throws IOException {
                collectLedgersFromStream(namespace, stream, ledgers);
            }
        });
    }

    private List<Long> collectLedgersFromStream(Namespace namespace,
                                                String stream,
                                                Set<Long> ledgers)
            throws IOException {
        DistributedLogManager dlm = namespace.openLog(stream);
        try {
            List<LogSegmentMetadata> segments = dlm.getLogSegments();
            List<Long> sLedgers = new ArrayList<Long>();
            for (LogSegmentMetadata segment : segments) {
                synchronized (ledgers) {
                    ledgers.add(segment.getLogSegmentId());
                }
                sLedgers.add(segment.getLogSegmentId());
            }
            return sLedgers;
        } finally {
            dlm.close();
        }
    }

    /**
     * Calculating stream space usage from given <i>uri</i>.
     *
     * @param uri dl uri
     * @throws IOException
     */
    public Map<String, Long> calculateStreamSpaceUsage(final URI uri) throws IOException {
        logger.info("Collecting stream space usage for {}.", uri);
        Namespace namespace = NamespaceBuilder.newBuilder()
                .conf(conf)
                .uri(uri)
                .build();
        try {
            return calculateStreamSpaceUsage(uri, namespace);
        } finally {
            namespace.close();
        }
    }

    private Map<String, Long> calculateStreamSpaceUsage(
            final URI uri, final Namespace namespace)
        throws IOException {
        Iterator<String> streams = namespace.getLogs();
        final LinkedBlockingQueue<String> streamQueue = new LinkedBlockingQueue<String>();
        while (streams.hasNext()) {
            streamQueue.add(streams.next());
        }

        final Map<String, Long> streamSpaceUsageMap =
                new ConcurrentSkipListMap<String, Long>();
        final AtomicInteger numStreamsCollected = new AtomicInteger(0);

        executeAction(streamQueue, 10, new Action<String>() {
            @Override
            public void execute(String stream) throws IOException {
                streamSpaceUsageMap.put(stream,
                        calculateStreamSpaceUsage(namespace, stream));
                if (numStreamsCollected.incrementAndGet() % 1000 == 0) {
                    logger.info("Calculated {} streams from uri {}.", numStreamsCollected.get(), uri);
                }
            }
        });

        return streamSpaceUsageMap;
    }

    private long calculateStreamSpaceUsage(final Namespace namespace,
                                           final String stream) throws IOException {
        DistributedLogManager dlm = namespace.openLog(stream);
        long totalBytes = 0;
        try {
            List<LogSegmentMetadata> segments = dlm.getLogSegments();
            for (LogSegmentMetadata segment : segments) {
                try {
                    LedgerHandle lh =
                            getBookKeeperClient(namespace).get().openLedgerNoRecovery(segment.getLogSegmentId(),
                            BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes(UTF_8));
                    totalBytes += lh.getLength();
                    lh.close();
                } catch (BKException e) {
                    logger.error("Failed to open ledger {} : ", segment.getLogSegmentId(), e);
                    throw new IOException("Failed to open ledger " + segment.getLogSegmentId(), e);
                } catch (InterruptedException e) {
                    logger.warn("Interrupted on opening ledger {} : ", segment.getLogSegmentId(), e);
                    Thread.currentThread().interrupt();
                    throw new DLInterruptedException("Interrupted on opening ledger " + segment.getLogSegmentId(), e);
                }
            }
        } finally {
            dlm.close();
        }
        return totalBytes;
    }

    public long calculateLedgerSpaceUsage(URI uri) throws IOException {
        List<URI> uris = Lists.newArrayList(uri);
        String zkServers = validateAndGetZKServers(uris);
        RetryPolicy retryPolicy = new BoundExponentialBackoffRetryPolicy(
                conf.getZKRetryBackoffStartMillis(),
                conf.getZKRetryBackoffMaxMillis(),
                Integer.MAX_VALUE);
        ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder()
                .name("DLAuditor-ZK")
                .zkServers(zkServers)
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .retryPolicy(retryPolicy)
                .zkAclId(conf.getZkAclId())
                .build();
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            BKDLConfig bkdlConfig = resolveBKDLConfig(zkc, uris);
            logger.info("Resolved bookkeeper config : {}", bkdlConfig);

            BookKeeperClient bkc = BookKeeperClientBuilder.newBuilder()
                    .name("DLAuditor-BK")
                    .dlConfig(conf)
                    .zkServers(bkdlConfig.getBkZkServersForWriter())
                    .ledgersPath(bkdlConfig.getBkLedgersPath())
                    .build();
            try {
                return calculateLedgerSpaceUsage(bkc, executorService);
            } finally {
                bkc.close();
            }
        } finally {
            zkc.close();
            executorService.shutdown();
        }
    }

    private long calculateLedgerSpaceUsage(BookKeeperClient bkc,
                                           final ExecutorService executorService)
        throws IOException {
        final AtomicLong totalBytes = new AtomicLong(0);
        final AtomicLong totalEntries = new AtomicLong(0);
        final AtomicLong numLedgers = new AtomicLong(0);

        LedgerManager lm = BookKeeperAccessor.getLedgerManager(bkc.get());

        final CompletableFuture<Void> doneFuture = FutureUtils.createFuture();
        final BookKeeper bk = bkc.get();

        BookkeeperInternalCallbacks.Processor<Long> collector =
                new BookkeeperInternalCallbacks.Processor<Long>() {
            @Override
            public void process(final Long lid,
                                final AsyncCallback.VoidCallback cb) {
                numLedgers.incrementAndGet();
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        bk.asyncOpenLedgerNoRecovery(lid,
                                BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes(UTF_8),
                                new org.apache.bookkeeper.client.AsyncCallback.OpenCallback() {
                            @Override
                            public void openComplete(int rc, LedgerHandle lh, Object ctx) {
                                final int cbRc;
                                if (BKException.Code.OK == rc) {
                                    totalBytes.addAndGet(lh.getLength());
                                    totalEntries.addAndGet(lh.getLastAddConfirmed() + 1);
                                    cbRc = rc;
                                } else {
                                    cbRc = BKException.Code.ZKException;
                                }
                                executorService.submit(new Runnable() {
                                    @Override
                                    public void run() {
                                        cb.processResult(cbRc, null, null);
                                    }
                                });
                            }
                        }, null);
                    }
                });
            }
        };
        AsyncCallback.VoidCallback finalCb = new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (BKException.Code.OK == rc) {
                    doneFuture.complete(null);
                } else {
                    doneFuture.completeExceptionally(BKException.create(rc));
                }
            }
        };
        lm.asyncProcessLedgers(collector, finalCb, null, BKException.Code.OK, BKException.Code.ZKException);
        try {
            doneFuture.get();
            logger.info("calculated {} ledgers\n\ttotal bytes = {}\n\ttotal entries = {}",
                numLedgers.get(), totalBytes.get(), totalEntries.get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DLInterruptedException("Interrupted on calculating ledger space : ", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) (e.getCause());
            } else {
                throw new IOException("Failed to calculate ledger space : ", e.getCause());
            }
        }
        return totalBytes.get();
    }

    public void close() {
        // no-op
    }

    interface Action<T> {
        void execute(T item) throws IOException;
    }

    static <T> void executeAction(final LinkedBlockingQueue<T> queue,
                                  final int numThreads,
                                  final Action<T> action) throws IOException {
        final CountDownLatch failureLatch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(queue.size());
        final AtomicInteger numFailures = new AtomicInteger(0);
        final AtomicInteger completedThreads = new AtomicInteger(0);

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        try {
            for (int i = 0; i < numThreads; i++) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        while (true) {
                            T item = queue.poll();
                            if (null == item) {
                                break;
                            }
                            try {
                                action.execute(item);
                            } catch (IOException ioe) {
                                logger.error("Failed to execute action on item '{}'", item, ioe);
                                numFailures.incrementAndGet();
                                failureLatch.countDown();
                                break;
                            }
                            doneLatch.countDown();
                        }
                        if (numFailures.get() == 0 && completedThreads.incrementAndGet() == numThreads) {
                            failureLatch.countDown();
                        }
                    }
                });
            }
            try {
                failureLatch.await();
                if (numFailures.get() > 0) {
                    throw new IOException("Encountered " + numFailures.get() + " failures on executing action.");
                }
                doneLatch.await();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted on executing action", ie);
                throw new DLInterruptedException("Interrupted on executing action", ie);
            }
        } finally {
            executorService.shutdown();
        }
    }

}
