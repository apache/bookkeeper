/*
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
 */
package org.apache.bookkeeper.statelib.impl.journal;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.statelib.api.AsyncStateStore;
import org.apache.bookkeeper.statelib.api.StateStore;
import org.apache.bookkeeper.statelib.api.StateStoreSpec;
import org.apache.bookkeeper.statelib.api.exceptions.StateStoreClosedException;
import org.apache.bookkeeper.statelib.api.exceptions.StateStoreException;
import org.apache.bookkeeper.statelib.api.exceptions.StateStoreRuntimeException;
import org.apache.bookkeeper.statelib.impl.Constants;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.bk.LedgerMetadata;
import org.apache.distributedlog.exceptions.LogEmptyException;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.apache.distributedlog.util.Utils;

/**
 * An abstract implementation of {@link AsyncStateStore} with journal.
 */
@Slf4j
public abstract class AbstractStateStoreWithJournal<LocalStateStoreT extends StateStore> implements AsyncStateStore {

    // local state store instance
    @Getter
    protected final LocalStateStoreT localStore;

    // parameters
    protected String name = "UNINITIALIZED";

    // spec
    protected StateStoreSpec spec;

    protected boolean ownWriteScheduler = false;
    protected boolean ownReadScheduler = false;
    protected ScheduledExecutorService writeIOScheduler;
    protected ScheduledExecutorService readIOScheduler;

    // journal
    private final Namespace logNamespace;
    private DistributedLogManager logManager;
    private AsyncLogWriter writer;
    private long nextRevision;
    private CommandProcessor<LocalStateStoreT> commandProcessor;

    // checkpoint
    private ScheduledFuture<?> checkpointTask;
    private Duration checkpointInterval;

    // close state
    protected boolean isInitialized = false;
    protected CompletableFuture<Void> closeFuture = null;

    protected AbstractStateStoreWithJournal(Supplier<LocalStateStoreT> localStateStoreSupplier,
                                            Supplier<Namespace> namespaceSupplier) {
        this.localStore = localStateStoreSupplier.get();
        this.logNamespace = namespaceSupplier.get();
    }

    protected boolean ownWriteScheduler() {
        return ownWriteScheduler;
    }

    protected boolean ownReadScheduler() {
        return ownReadScheduler;
    }

    synchronized AsyncLogWriter getWriter() {
        return writer;
    }

    private void validateStoreSpec(StateStoreSpec spec) {
        checkNotNull(
            spec.getStream(),
            "No log stream is specified for state store %s", spec.getName());
    }

    private synchronized void markInitialized(AsyncLogReader reader) {
        isInitialized = true;
        // schedule periodical checkpoint
        if (null != checkpointInterval) {
            long checkpointIntervalMs = checkpointInterval.toMillis();
            checkpointTask = writeIOScheduler.scheduleAtFixedRate(
                () -> localStore.checkpoint(),
                checkpointIntervalMs,
                checkpointIntervalMs,
                TimeUnit.MILLISECONDS);
        }
        if (spec.isReadonly()) {
            replayLoop(reader);
        } else {
            reader.asyncClose();
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public StateStoreSpec spec() {
        return spec;
    }

    @Override
    public CompletableFuture<Void> init(StateStoreSpec spec) {
        try {
            validateStoreSpec(spec);
        } catch (IllegalArgumentException e) {
            log.error("Fail to init state store due to : ", e);
            return FutureUtils.exception(e);
        }
        this.spec = spec;
        this.name = spec.getName();

        if (null != spec.getWriteIOScheduler()) {
            writeIOScheduler = spec.getWriteIOScheduler();
            ownWriteScheduler = false;
        } else {
            ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("statestore-" + spec.getName() + "-write-io-scheduler-%d")
                .build();
            writeIOScheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
            ownWriteScheduler = true;
        }

        if (null != spec.getReadIOScheduler()) {
            readIOScheduler = spec.getReadIOScheduler();
        } else if (ownWriteScheduler) {
            readIOScheduler = writeIOScheduler;
            ownReadScheduler = false;
        } else {
            ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("statestore-" + spec.getName() + "-read-io-scheduler-%d")
                .build();
            readIOScheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
            ownReadScheduler = true;
        }

        if (null != spec.getCheckpointStore()) {
            this.checkpointInterval = spec.getCheckpointDuration();
        } else {
            this.checkpointInterval = null;
        }

        if (spec.isReadonly()) {
            return initializeLocalStore(spec)
                .thenComposeAsync(ignored -> getLastDLSN(spec), writeIOScheduler)
                .thenComposeAsync(endDLSN -> replayJournal(endDLSN), writeIOScheduler);
        } else {
            return initializeLocalStore(spec)
                .thenComposeAsync(ignored -> initializeJournalWriter(spec), writeIOScheduler)
                .thenComposeAsync(endDLSN -> {
                    log.info("Successfully write a barrier record for mvcc store {} at {}", name, endDLSN);
                    return replayJournal(endDLSN);
                }, writeIOScheduler);
        }
    }

    private CompletableFuture<Void> initializeLocalStore(StateStoreSpec spec) {
        return executeWriteIO(() -> {
            log.info("Initializing the local state for mvcc store {}", name());
            localStore.init(spec);
            log.info("Initialized the local state for mvcc store {}", name());
            commandProcessor = newCommandProcessor();
            return null;
        });
    }

    private CompletableFuture<DLSN> initializeJournalWriter(StateStoreSpec spec) {
        synchronized (this) {
            if (null != closeFuture) {
                return FutureUtils.exception(new StateStoreClosedException(name()));
            }
        }

        try {
            logManager = logNamespace.openLog(spec.getStream());
        } catch (IOException e) {
            return FutureUtils.exception(e);
        }
        LedgerMetadata metadata = new LedgerMetadata();
        metadata.setApplication(Constants.LEDGER_METADATA_APPLICATION_STREAM_STORAGE);
        metadata.setComponent("state-store");
        return logManager.openAsyncLogWriter(metadata).thenComposeAsync(w -> {
            synchronized (this) {
                writer = w;
                nextRevision = writer.getLastTxId();
                if (nextRevision < 0) {
                    nextRevision = 0L;
                }
                log.info("Initialized the journal writer for mvcc store {} : last revision = {}",
                    name(), nextRevision);
            }
            return writeCommandBuf(newCatchupMarker());
        }, writeIOScheduler);
    }

    private CompletableFuture<DLSN> writeCatchUpMarker() {
        LedgerMetadata metadata = new LedgerMetadata();
        metadata.setApplication(Constants.LEDGER_METADATA_APPLICATION_STREAM_STORAGE);
        metadata.setComponent("state-store");
        return logManager.openAsyncLogWriter(metadata).thenComposeAsync(w -> {
            synchronized (this) {
                writer = w;
                nextRevision = writer.getLastTxId();
                if (nextRevision < 0) {
                    nextRevision = 0L;
                }
                log.info("Initialized the journal writer for writing catchup marker to mvcc store {} :"
                    + " last revision = {}", name(), nextRevision);
            }
            return writeCommandBuf(newCatchupMarker());
        }).thenCompose(dlsn -> {
            AsyncLogWriter w;
            synchronized (this) {
                w = writer;
            }
            if (null == w) {
                return FutureUtils.value(dlsn);
            } else {
                return w.asyncClose().thenApply(ignored -> dlsn);
            }
        });
    }

    private CompletableFuture<DLSN> getLastDLSN(StateStoreSpec spec) {
        synchronized (this) {
            if (null != closeFuture) {
                return FutureUtils.exception(new StateStoreClosedException(name()));
            }
        }

        try {
            logManager = logNamespace.openLog(spec.getStream());
        } catch (IOException e) {
            return FutureUtils.exception(e);
        }
        CompletableFuture<DLSN> future = FutureUtils.createFuture();
        logManager.getLastDLSNAsync().whenCompleteAsync(new FutureEventListener<DLSN>() {
            @Override
            public void onSuccess(DLSN dlsn) {
                future.complete(dlsn);
            }

            @Override
            public void onFailure(Throwable throwable) {
                if (throwable instanceof LogEmptyException || throwable instanceof LogNotFoundException) {
                    FutureUtils.proxyTo(
                        writeCatchUpMarker(),
                        future);
                } else {
                    future.completeExceptionally(throwable);
                }
            }
        });
        return future;
    }

    private CompletableFuture<Void> replayJournal(DLSN endDLSN) {
        long lastRevision = localStore.getLastRevision();
        return logManager.openAsyncLogReader(lastRevision)
            .thenComposeAsync(r -> {
                CompletableFuture<Void> replayFuture = FutureUtils.createFuture();
                replayFuture.exceptionally(
                    cause -> {
                        r.asyncClose();
                        return null;
                    });

                log.info("Successfully open the journal reader for mvcc store {} : end dlsn = {}", name(), endDLSN);
                replayJournal(r, endDLSN, replayFuture);
                return replayFuture;
            }, writeIOScheduler);
    }

    private void replayJournal(AsyncLogReader reader,
                               DLSN endDLSN,
                               CompletableFuture<Void> future) {
        synchronized (this) {
            if (null != closeFuture) {
                FutureUtils.completeExceptionally(future, new StateStoreClosedException(name()));
                return;
            }
        }

        reader.readNext().whenComplete(new FutureEventListener<LogRecordWithDLSN>() {
            @Override
            public void onSuccess(LogRecordWithDLSN record) {
                if (log.isDebugEnabled()) {
                    log.debug("Received command record {} @ {} to replay at mvcc store {}",
                        record, record.getDlsn(), name());
                }
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Applying command transaction {} - record {} @ {} to mvcc store {}",
                            record.getTransactionId(), record, record.getDlsn(), name());
                    }
                    commandProcessor.applyCommand(record.getTransactionId(), record.getPayloadBuf(), localStore);

                    if (record.getDlsn().compareTo(endDLSN) >= 0) {
                        log.info("Finished replaying journal for state store {}", name());
                        markInitialized(reader);
                        FutureUtils.complete(future, null);
                        return;
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Read next record after {} at mvcc store {}",
                            record.getDlsn(), name());
                    }
                    // read next record
                    replayJournal(reader, endDLSN, future);
                } catch (Exception e) {
                    log.error("Exception is thrown when applying command record {} @ {} to mvcc store {}",
                        record, record.getDlsn(), name());
                    FutureUtils.completeExceptionally(future, e);
                }
            }

            @Override
            public void onFailure(Throwable cause) {
                FutureUtils.completeExceptionally(future, cause);
            }
        });
    }

    private void replayLoop(AsyncLogReader reader) {
        synchronized (this) {
            if (null != closeFuture) {
                reader.asyncClose();
                return;
            }
        }

        reader.readNext().whenComplete(new FutureEventListener<LogRecordWithDLSN>() {
            @Override
            public void onSuccess(LogRecordWithDLSN record) {
                if (log.isDebugEnabled()) {
                    log.debug("Received command record {} @ {} to replay at mvcc store {}",
                        record, record.getDlsn(), name());
                }
                try {
                    commandProcessor.applyCommand(record.getTransactionId(), record.getPayloadBuf(), localStore);
                    // read next record
                    replayLoop(reader);
                } catch (StateStoreRuntimeException e) {
                    log.error("Fail to reply command record {}", record, e);
                    // TODO: handle state store exception
                }
            }

            @Override
            public void onFailure(Throwable throwable) {
                // TODO: handle read failure
            }
        });
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> future;
        synchronized (this) {
            if (null != closeFuture) {
                return closeFuture;
            }
            closeFuture = future = FutureUtils.createFuture();
        }

        // cancel checkpoint task
        if (null != checkpointTask) {
            if (!checkpointTask.cancel(true)) {
                log.warn("Fail to cancel checkpoint task of state store {}", name());
            }
        }
        // wait until last checkpoint task completed
        writeIOScheduler.submit(() -> {
            log.info("closing async state store {}", name);
            FutureUtils.ensure(
                // close the log streams
                Utils.closeSequence(
                    readIOScheduler,
                    true,
                    getWriter(),
                    logManager
                ).thenRun(() -> {
                    log.info("Successfully close the log stream of state store {}", name);
                }),
                // close the local state store
                () -> {
                    if (null == readIOScheduler) {
                        closeLocalStore();
                        FutureUtils.complete(future, null);
                        return;
                    }
                    readIOScheduler.submit(() -> {
                        closeLocalStore();

                        if (ownReadScheduler) {
                            readIOScheduler.shutdown();
                        }
                        if (ownWriteScheduler) {
                            writeIOScheduler.shutdown();
                        }
                        FutureUtils.complete(future, null);
                    });
                }
            );
        });
        return future;
    }

    private void closeLocalStore() {
        if (null == localStore) {
            return;
        }
        // flush the local store
        try {
            localStore.flush();
        } catch (StateStoreException e) {
            log.warn("Fail to flush local state store {}", name(), e);
        }
        // close the local store
        localStore.close();
    }

    private <T> CompletableFuture<T> executeIO(ScheduledExecutorService scheduler,
                                               Callable<T> callable) {
        synchronized (this) {
            if (null != closeFuture) {
                return FutureUtils.exception(new StateStoreClosedException(name()));
            }
        }

        CompletableFuture<T> future = FutureUtils.createFuture();
        scheduler.submit(() -> {
            try {
                T value = callable.call();
                future.complete(value);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    //
    // Operations exposed to the implementation
    //

    protected <T> CompletableFuture<T> executeWriteIO(Callable<T> callable) {
        return executeIO(writeIOScheduler, callable);
    }

    protected <T> CompletableFuture<T> executeReadIO(Callable<T> callable) {
        return executeIO(readIOScheduler, callable);
    }

    protected synchronized CompletableFuture<DLSN> writeCommandBuf(ByteBuf cmdBuf) {
        long txId = ++nextRevision;
        return FutureUtils.ensure(
            writer.write(new LogRecord(txId, cmdBuf.nioBuffer())),
            () -> cmdBuf.release());
    }

    protected synchronized CompletableFuture<Long> writeCommandBufReturnTxId(ByteBuf cmdBuf) {
        long txId = ++nextRevision;
        return FutureUtils.ensure(
            writer.write(new LogRecord(txId, cmdBuf.nioBuffer()))
                .thenApply(dlsn -> txId),
            () -> cmdBuf.release());
    }

    //
    // Methods to implement
    //

    /**
     * Create a catchup marker and append the marker to the journal when initializing the journal.
     *
     * <p>The catchup marker will then be used as the end marker for replaying journal.
     *
     * <p>The implementation of this class should be able to process the catch up marker and ignore it.
     *
     * @return a catchup marker.
     */
    protected abstract ByteBuf newCatchupMarker();

    /**
     * Create a new command processor for processing the commands replayed from the journal.
     *
     * @return a new command processor.
     */
    protected abstract CommandProcessor<LocalStateStoreT> newCommandProcessor();
}
