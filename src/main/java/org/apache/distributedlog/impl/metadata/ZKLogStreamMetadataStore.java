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
package org.apache.distributedlog.impl.metadata;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.DistributedLogConstants;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.exceptions.DLInterruptedException;
import org.apache.distributedlog.exceptions.InvalidStreamNameException;
import org.apache.distributedlog.exceptions.LockCancelledException;
import org.apache.distributedlog.exceptions.LogExistsException;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.apache.distributedlog.exceptions.UnexpectedException;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.impl.ZKLogSegmentMetadataStore;
import org.apache.distributedlog.lock.DistributedLock;
import org.apache.distributedlog.lock.SessionLockFactory;
import org.apache.distributedlog.lock.ZKDistributedLock;
import org.apache.distributedlog.lock.ZKSessionLockFactory;
import org.apache.distributedlog.logsegment.LogSegmentMetadataStore;
import org.apache.distributedlog.metadata.LogStreamMetadataStore;
import org.apache.distributedlog.metadata.LogMetadata;
import org.apache.distributedlog.metadata.LogMetadataForReader;
import org.apache.distributedlog.metadata.LogMetadataForWriter;
import org.apache.distributedlog.util.DLUtils;
import org.apache.distributedlog.common.concurrent.FutureUtils;
import org.apache.distributedlog.common.util.SchedulerUtils;
import org.apache.distributedlog.zk.LimitedPermitManager;
import org.apache.distributedlog.util.OrderedScheduler;
import org.apache.distributedlog.common.util.PermitManager;
import org.apache.distributedlog.util.Transaction;
import org.apache.distributedlog.util.Utils;
import org.apache.distributedlog.zk.ZKTransaction;
import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.distributedlog.metadata.LogMetadata.*;

/**
 * zookeeper based {@link LogStreamMetadataStore}
 */
public class ZKLogStreamMetadataStore implements LogStreamMetadataStore {

    private final static Logger LOG = LoggerFactory.getLogger(ZKLogStreamMetadataStore.class);

    private final String clientId;
    private final DistributedLogConfiguration conf;
    private final ZooKeeperClient zooKeeperClient;
    private final OrderedScheduler scheduler;
    private final StatsLogger statsLogger;
    private final LogSegmentMetadataStore logSegmentStore;
    private final LimitedPermitManager permitManager;
    // lock
    private SessionLockFactory lockFactory;
    private OrderedScheduler lockStateExecutor;

    public ZKLogStreamMetadataStore(String clientId,
                                    DistributedLogConfiguration conf,
                                    ZooKeeperClient zkc,
                                    OrderedScheduler scheduler,
                                    StatsLogger statsLogger) {
        this.clientId = clientId;
        this.conf = conf;
        this.zooKeeperClient = zkc;
        this.scheduler = scheduler;
        this.statsLogger = statsLogger;
        // create the log segment metadata store and the permit manager (used for log segment rolling)
        this.logSegmentStore = new ZKLogSegmentMetadataStore(conf, zooKeeperClient, scheduler);
        this.permitManager = new LimitedPermitManager(
                conf.getLogSegmentRollingConcurrency(),
                1,
                TimeUnit.MINUTES,
                scheduler);
        this.zooKeeperClient.register(permitManager);
    }

    private synchronized OrderedScheduler getLockStateExecutor(boolean createIfNull) {
        if (createIfNull && null == lockStateExecutor) {
            lockStateExecutor = OrderedScheduler.newBuilder()
                    .name("DLM-LockState")
                    .corePoolSize(conf.getNumLockStateThreads())
                    .build();
        }
        return lockStateExecutor;
    }

    private synchronized SessionLockFactory getLockFactory(boolean createIfNull) {
        if (createIfNull && null == lockFactory) {
            lockFactory = new ZKSessionLockFactory(
                    zooKeeperClient,
                    clientId,
                    getLockStateExecutor(createIfNull),
                    conf.getZKNumRetries(),
                    conf.getLockTimeoutMilliSeconds(),
                    conf.getZKRetryBackoffStartMillis(),
                    statsLogger);
        }
        return lockFactory;
    }

    @Override
    public void close() throws IOException {
        this.zooKeeperClient.unregister(permitManager);
        this.permitManager.close();
        this.logSegmentStore.close();
        SchedulerUtils.shutdownScheduler(
                getLockStateExecutor(false),
                conf.getSchedulerShutdownTimeoutMs(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public LogSegmentMetadataStore getLogSegmentMetadataStore() {
        return logSegmentStore;
    }

    @Override
    public PermitManager getPermitManager() {
        return this.permitManager;
    }

    @Override
    public Transaction<Object> newTransaction() {
        return new ZKTransaction(zooKeeperClient);
    }

    @Override
    public CompletableFuture<Void> logExists(URI uri, final String logName) {
        final String logSegmentsPath = LogMetadata.getLogSegmentsPath(
                uri, logName, conf.getUnpartitionedStreamName());
        final CompletableFuture<Void> promise = new CompletableFuture<Void>();
        try {
            final ZooKeeper zk = zooKeeperClient.get();
            zk.sync(logSegmentsPath, new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int syncRc, String path, Object syncCtx) {
                    if (KeeperException.Code.NONODE.intValue() == syncRc) {
                        promise.completeExceptionally(new LogNotFoundException(
                                String.format("Log %s does not exist or has been deleted", logName)));
                        return;
                    } else if (KeeperException.Code.OK.intValue() != syncRc){
                        promise.completeExceptionally(new ZKException("Error on checking log existence for " + logName,
                                KeeperException.create(KeeperException.Code.get(syncRc))));
                        return;
                    }
                    zk.exists(logSegmentsPath, false, new AsyncCallback.StatCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx, Stat stat) {
                            if (KeeperException.Code.OK.intValue() == rc) {
                                promise.complete(null);
                            } else if (KeeperException.Code.NONODE.intValue() == rc) {
                                promise.completeExceptionally(new LogNotFoundException(
                                        String.format("Log %s does not exist or has been deleted", logName)));
                            } else {
                                promise.completeExceptionally(new ZKException("Error on checking log existence for " + logName,
                                        KeeperException.create(KeeperException.Code.get(rc))));
                            }
                        }
                    }, null);
                }
            }, null);

        } catch (InterruptedException ie) {
            LOG.error("Interrupted while reading {}", logSegmentsPath, ie);
            promise.completeExceptionally(new DLInterruptedException("Interrupted while checking "
                    + logSegmentsPath, ie));
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            promise.completeExceptionally(e);
        }
        return promise;
    }

    //
    // Create Write Lock
    //

    @Override
    public DistributedLock createWriteLock(LogMetadataForWriter metadata) {
        return new ZKDistributedLock(
                getLockStateExecutor(true),
                getLockFactory(true),
                metadata.getLockPath(),
                conf.getLockTimeoutMilliSeconds(),
                statsLogger);
    }

    //
    // Create Read Lock
    //

    private CompletableFuture<Void> ensureReadLockPathExist(final LogMetadata logMetadata,
                                                 final String readLockPath) {
        final CompletableFuture<Void> promise = new CompletableFuture<Void>();
        promise.whenComplete((value, cause) -> {
            if (cause instanceof CancellationException) {
                FutureUtils.completeExceptionally(promise, new LockCancelledException(readLockPath,
                        "Could not ensure read lock path", cause));
            }
        });
        Optional<String> parentPathShouldNotCreate = Optional.of(logMetadata.getLogRootPath());
        Utils.zkAsyncCreateFullPathOptimisticRecursive(zooKeeperClient, readLockPath, parentPathShouldNotCreate,
                new byte[0], zooKeeperClient.getDefaultACL(), CreateMode.PERSISTENT,
                new org.apache.zookeeper.AsyncCallback.StringCallback() {
                    @Override
                    public void processResult(final int rc, final String path, Object ctx, String name) {
                        if (KeeperException.Code.NONODE.intValue() == rc) {
                            FutureUtils.completeExceptionally(promise, new LogNotFoundException(
                                    String.format("Log %s does not exist or has been deleted",
                                            logMetadata.getFullyQualifiedName())));
                        } else if (KeeperException.Code.OK.intValue() == rc) {
                            FutureUtils.complete(promise, null);
                            LOG.trace("Created path {}.", path);
                        } else if (KeeperException.Code.NODEEXISTS.intValue() == rc) {
                            FutureUtils.complete(promise, null);
                            LOG.trace("Path {} is already existed.", path);
                        } else if (DistributedLogConstants.ZK_CONNECTION_EXCEPTION_RESULT_CODE == rc) {
                            FutureUtils.completeExceptionally(promise, new ZooKeeperClient.ZooKeeperConnectionException(path));
                        } else if (DistributedLogConstants.DL_INTERRUPTED_EXCEPTION_RESULT_CODE == rc) {
                            FutureUtils.completeExceptionally(promise, new DLInterruptedException(path));
                        } else {
                            FutureUtils.completeExceptionally(promise, KeeperException.create(KeeperException.Code.get(rc)));
                        }
                    }
                }, null);
        return promise;
    }

    @Override
    public CompletableFuture<DistributedLock> createReadLock(final LogMetadataForReader metadata,
                                                  Optional<String> readerId) {
        final String readLockPath = metadata.getReadLockPath(readerId);
        return ensureReadLockPathExist(metadata, readLockPath)
            .thenApplyAsync((value) -> {
                DistributedLock lock = new ZKDistributedLock(
                    getLockStateExecutor(true),
                    getLockFactory(true),
                    readLockPath,
                    conf.getLockTimeoutMilliSeconds(),
                    statsLogger.scope("read_lock"));
                return lock;
            }, scheduler.chooseExecutor(readLockPath));
    }

    //
    // Create Log
    //

    static class MetadataIndex {
        static final int LOG_ROOT_PARENT = 0;
        static final int LOG_ROOT = 1;
        static final int MAX_TXID = 2;
        static final int VERSION = 3;
        static final int LOCK = 4;
        static final int READ_LOCK = 5;
        static final int LOGSEGMENTS = 6;
        static final int ALLOCATION = 7;
    }

    static int bytesToInt(byte[] b) {
        assert b.length >= 4;
        return b[0] << 24 | b[1] << 16 | b[2] << 8 | b[3];
    }

    static byte[] intToBytes(int i) {
        return new byte[]{
            (byte) (i >> 24),
            (byte) (i >> 16),
            (byte) (i >> 8),
            (byte) (i)};
    }

    static CompletableFuture<List<Versioned<byte[]>>> checkLogMetadataPaths(ZooKeeper zk,
                                                                 String logRootPath,
                                                                 boolean ownAllocator) {
        // Note re. persistent lock state initialization: the read lock persistent state (path) is
        // initialized here but only used in the read handler. The reason is its more convenient and
        // less error prone to manage all stream structure in one place.
        final String logRootParentPath = Utils.getParent(logRootPath);
        final String logSegmentsPath = logRootPath + LOGSEGMENTS_PATH;
        final String maxTxIdPath = logRootPath + MAX_TXID_PATH;
        final String lockPath = logRootPath + LOCK_PATH;
        final String readLockPath = logRootPath + READ_LOCK_PATH;
        final String versionPath = logRootPath + VERSION_PATH;
        final String allocationPath = logRootPath + ALLOCATION_PATH;

        int numPaths = ownAllocator ? MetadataIndex.ALLOCATION + 1 : MetadataIndex.LOGSEGMENTS + 1;
        List<CompletableFuture<Versioned<byte[]>>> checkFutures = Lists.newArrayListWithExpectedSize(numPaths);
        checkFutures.add(Utils.zkGetData(zk, logRootParentPath, false));
        checkFutures.add(Utils.zkGetData(zk, logRootPath, false));
        checkFutures.add(Utils.zkGetData(zk, maxTxIdPath, false));
        checkFutures.add(Utils.zkGetData(zk, versionPath, false));
        checkFutures.add(Utils.zkGetData(zk, lockPath, false));
        checkFutures.add(Utils.zkGetData(zk, readLockPath, false));
        checkFutures.add(Utils.zkGetData(zk, logSegmentsPath, false));
        if (ownAllocator) {
            checkFutures.add(Utils.zkGetData(zk, allocationPath, false));
        }

        return FutureUtils.collect(checkFutures);
    }

    static boolean pathExists(Versioned<byte[]> metadata) {
        return null != metadata.getValue() && null != metadata.getVersion();
    }

    static void ensureMetadataExist(Versioned<byte[]> metadata) {
        Preconditions.checkNotNull(metadata.getValue());
        Preconditions.checkNotNull(metadata.getVersion());
    }

    static void createMissingMetadata(final ZooKeeper zk,
                                      final String logRootPath,
                                      final List<Versioned<byte[]>> metadatas,
                                      final List<ACL> acl,
                                      final boolean ownAllocator,
                                      final boolean createIfNotExists,
                                      final CompletableFuture<List<Versioned<byte[]>>> promise) {
        final List<byte[]> pathsToCreate = Lists.newArrayListWithExpectedSize(metadatas.size());
        final List<Op> zkOps = Lists.newArrayListWithExpectedSize(metadatas.size());
        CreateMode createMode = CreateMode.PERSISTENT;

        // log root parent path
        if (pathExists(metadatas.get(MetadataIndex.LOG_ROOT_PARENT))) {
            pathsToCreate.add(null);
        } else {
            String logRootParentPath = Utils.getParent(logRootPath);
            pathsToCreate.add(DistributedLogConstants.EMPTY_BYTES);
            zkOps.add(Op.create(logRootParentPath, DistributedLogConstants.EMPTY_BYTES, acl, createMode));
        }

        // log root path
        if (pathExists(metadatas.get(MetadataIndex.LOG_ROOT))) {
            pathsToCreate.add(null);
        } else {
            pathsToCreate.add(DistributedLogConstants.EMPTY_BYTES);
            zkOps.add(Op.create(logRootPath, DistributedLogConstants.EMPTY_BYTES, acl, createMode));
        }

        // max id
        if (pathExists(metadatas.get(MetadataIndex.MAX_TXID))) {
            pathsToCreate.add(null);
        } else {
            byte[] zeroTxnIdData = DLUtils.serializeTransactionId(0L);
            pathsToCreate.add(zeroTxnIdData);
            zkOps.add(Op.create(logRootPath + MAX_TXID_PATH, zeroTxnIdData, acl, createMode));
        }
        // version
        if (pathExists(metadatas.get(MetadataIndex.VERSION))) {
            pathsToCreate.add(null);
        } else {
            byte[] versionData = intToBytes(LAYOUT_VERSION);
            pathsToCreate.add(versionData);
            zkOps.add(Op.create(logRootPath + VERSION_PATH, versionData, acl, createMode));
        }
        // lock path
        if (pathExists(metadatas.get(MetadataIndex.LOCK))) {
            pathsToCreate.add(null);
        } else {
            pathsToCreate.add(DistributedLogConstants.EMPTY_BYTES);
            zkOps.add(Op.create(logRootPath + LOCK_PATH, DistributedLogConstants.EMPTY_BYTES, acl, createMode));
        }
        // read lock path
        if (pathExists(metadatas.get(MetadataIndex.READ_LOCK))) {
            pathsToCreate.add(null);
        } else {
            pathsToCreate.add(DistributedLogConstants.EMPTY_BYTES);
            zkOps.add(Op.create(logRootPath + READ_LOCK_PATH, DistributedLogConstants.EMPTY_BYTES, acl, createMode));
        }
        // log segments path
        if (pathExists(metadatas.get(MetadataIndex.LOGSEGMENTS))) {
            pathsToCreate.add(null);
        } else {
            byte[] logSegmentsData = DLUtils.serializeLogSegmentSequenceNumber(
                    DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO);
            pathsToCreate.add(logSegmentsData);
            zkOps.add(Op.create(logRootPath + LOGSEGMENTS_PATH, logSegmentsData, acl, createMode));
        }
        // allocation path
        if (ownAllocator) {
            if (pathExists(metadatas.get(MetadataIndex.ALLOCATION))) {
                pathsToCreate.add(null);
            } else {
                pathsToCreate.add(DistributedLogConstants.EMPTY_BYTES);
                zkOps.add(Op.create(logRootPath + ALLOCATION_PATH,
                        DistributedLogConstants.EMPTY_BYTES, acl, createMode));
            }
        }
        if (zkOps.isEmpty()) {
            // nothing missed
            promise.complete(metadatas);
            return;
        }
        if (!createIfNotExists) {
            promise.completeExceptionally(new LogNotFoundException("Log " + logRootPath + " not found"));
            return;
        }

        zk.multi(zkOps, new AsyncCallback.MultiCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<OpResult> resultList) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    List<Versioned<byte[]>> finalMetadatas =
                            Lists.newArrayListWithExpectedSize(metadatas.size());
                    for (int i = 0; i < pathsToCreate.size(); i++) {
                        byte[] dataCreated = pathsToCreate.get(i);
                        if (null == dataCreated) {
                            finalMetadatas.add(metadatas.get(i));
                        } else {
                            finalMetadatas.add(new Versioned<byte[]>(dataCreated, new ZkVersion(0)));
                        }
                    }
                    promise.complete(finalMetadatas);
                } else if (KeeperException.Code.NODEEXISTS.intValue() == rc) {
                    promise.completeExceptionally(new LogExistsException("Someone just created log "
                            + logRootPath));
                } else {
                    if (LOG.isDebugEnabled()) {
                        StringBuilder builder = new StringBuilder();
                        for (OpResult result : resultList) {
                            if (result instanceof OpResult.ErrorResult) {
                                OpResult.ErrorResult errorResult = (OpResult.ErrorResult) result;
                                builder.append(errorResult.getErr()).append(",");
                            } else {
                                builder.append(0).append(",");
                            }
                        }
                        String resultCodeList = builder.substring(0, builder.length() - 1);
                        LOG.debug("Failed to create log, full rc list = {}", resultCodeList);
                    }

                    promise.completeExceptionally(new ZKException("Failed to create log " + logRootPath,
                            KeeperException.Code.get(rc)));
                }
            }
        }, null);
    }

    static LogMetadataForWriter processLogMetadatas(URI uri,
                                                    String logName,
                                                    String logIdentifier,
                                                    List<Versioned<byte[]>> metadatas,
                                                    boolean ownAllocator)
            throws UnexpectedException {
        try {
            // max id
            Versioned<byte[]> maxTxnIdData = metadatas.get(MetadataIndex.MAX_TXID);
            ensureMetadataExist(maxTxnIdData);
            // version
            Versioned<byte[]> versionData = metadatas.get(MetadataIndex.VERSION);
            ensureMetadataExist(maxTxnIdData);
            Preconditions.checkArgument(LAYOUT_VERSION == bytesToInt(versionData.getValue()));
            // lock path
            ensureMetadataExist(metadatas.get(MetadataIndex.LOCK));
            // read lock path
            ensureMetadataExist(metadatas.get(MetadataIndex.READ_LOCK));
            // max lssn
            Versioned<byte[]> maxLSSNData = metadatas.get(MetadataIndex.LOGSEGMENTS);
            ensureMetadataExist(maxLSSNData);
            try {
                DLUtils.deserializeLogSegmentSequenceNumber(maxLSSNData.getValue());
            } catch (NumberFormatException nfe) {
                throw new UnexpectedException("Invalid max sequence number found in log " + logName, nfe);
            }
            // allocation path
            Versioned<byte[]>  allocationData;
            if (ownAllocator) {
                allocationData = metadatas.get(MetadataIndex.ALLOCATION);
                ensureMetadataExist(allocationData);
            } else {
                allocationData = new Versioned<byte[]>(null, null);
            }
            return new LogMetadataForWriter(uri, logName, logIdentifier,
                    maxLSSNData, maxTxnIdData, allocationData);
        } catch (IllegalArgumentException iae) {
            throw new UnexpectedException("Invalid log " + logName, iae);
        } catch (NullPointerException npe) {
            throw new UnexpectedException("Invalid log " + logName, npe);
        }
    }

    static CompletableFuture<LogMetadataForWriter> getLog(final URI uri,
                                               final String logName,
                                               final String logIdentifier,
                                               final ZooKeeperClient zooKeeperClient,
                                               final boolean ownAllocator,
                                               final boolean createIfNotExists) {
        final String logRootPath = LogMetadata.getLogRootPath(uri, logName, logIdentifier);
        try {
            PathUtils.validatePath(logRootPath);
        } catch (IllegalArgumentException e) {
            LOG.error("Illegal path value {} for stream {}", new Object[]{logRootPath, logName, e});
            return FutureUtils.exception(new InvalidStreamNameException(logName, "Log name is invalid"));
        }

        try {
            final ZooKeeper zk = zooKeeperClient.get();
            return checkLogMetadataPaths(zk, logRootPath, ownAllocator)
                    .thenCompose(new Function<List<Versioned<byte[]>>, CompletableFuture<List<Versioned<byte[]>>>>() {
                        @Override
                        public CompletableFuture<List<Versioned<byte[]>>> apply(List<Versioned<byte[]>> metadatas) {
                            CompletableFuture<List<Versioned<byte[]>>> promise =
                                    new CompletableFuture<List<Versioned<byte[]>>>();
                            createMissingMetadata(zk, logRootPath, metadatas, zooKeeperClient.getDefaultACL(),
                                    ownAllocator, createIfNotExists, promise);
                            return promise;
                        }
                    }).thenCompose(new Function<List<Versioned<byte[]>>, CompletableFuture<LogMetadataForWriter>>() {
                        @Override
                        public CompletableFuture<LogMetadataForWriter> apply(List<Versioned<byte[]>> metadatas) {
                            try {
                                return FutureUtils.value(
                                    processLogMetadatas(
                                        uri,
                                        logName,
                                        logIdentifier,
                                        metadatas,
                                        ownAllocator));
                            } catch (UnexpectedException e) {
                                return FutureUtils.exception(e);
                            }
                        }
                    });
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            return FutureUtils.exception(new ZKException("Encountered zookeeper connection issue on creating log " + logName,
                    KeeperException.Code.CONNECTIONLOSS));
        } catch (InterruptedException e) {
            return FutureUtils.exception(new DLInterruptedException("Interrupted on creating log " + logName, e));
        }
    }

    @Override
    public CompletableFuture<LogMetadataForWriter> getLog(final URI uri,
                                               final String logName,
                                               final boolean ownAllocator,
                                               final boolean createIfNotExists) {
        return getLog(
                uri,
                logName,
                conf.getUnpartitionedStreamName(),
                zooKeeperClient,
                ownAllocator,
                createIfNotExists);
    }

    //
    // Delete Log
    //

    @Override
    public CompletableFuture<Void> deleteLog(URI uri, final String logName) {
        final CompletableFuture<Void> promise = new CompletableFuture<Void>();
        try {
            String streamPath = LogMetadata.getLogStreamPath(uri, logName);
            ZKUtil.deleteRecursive(zooKeeperClient.get(), streamPath, new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx) {
                    if (KeeperException.Code.OK.intValue() != rc) {
                        FutureUtils.completeExceptionally(promise,
                                new ZKException("Encountered zookeeper issue on deleting log stream "
                                        + logName, KeeperException.Code.get(rc)));
                        return;
                    }
                    FutureUtils.complete(promise, null);
                }
            }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            FutureUtils.completeExceptionally(promise, new ZKException("Encountered zookeeper issue on deleting log stream "
                    + logName, KeeperException.Code.CONNECTIONLOSS));
        } catch (InterruptedException e) {
            FutureUtils.completeExceptionally(promise, new DLInterruptedException("Interrupted while deleting log stream "
                    + logName));
        } catch (KeeperException e) {
            FutureUtils.completeExceptionally(promise, new ZKException("Encountered zookeeper issue on deleting log stream "
                    + logName, e));
        }
        return promise;
    }
}
