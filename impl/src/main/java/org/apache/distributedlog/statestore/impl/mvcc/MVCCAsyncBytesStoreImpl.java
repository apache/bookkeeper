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

package org.apache.distributedlog.statestore.impl.mvcc;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.distributedlog.statestore.impl.mvcc.MVCCUtils.NOP_CMD;
import static org.apache.distributedlog.statestore.impl.mvcc.MVCCUtils.newCommand;
import static org.apache.distributedlog.statestore.impl.mvcc.MVCCUtils.toCommand;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.statestore.api.StateStoreSpec;
import org.apache.distributedlog.statestore.api.mvcc.MVCCAsyncStore;
import org.apache.distributedlog.statestore.api.mvcc.op.DeleteOp;
import org.apache.distributedlog.statestore.api.mvcc.op.OpFactory;
import org.apache.distributedlog.statestore.api.mvcc.op.PutOp;
import org.apache.distributedlog.statestore.api.mvcc.op.RangeOp;
import org.apache.distributedlog.statestore.api.mvcc.op.TxnOp;
import org.apache.distributedlog.statestore.api.mvcc.result.Code;
import org.apache.distributedlog.statestore.api.mvcc.result.DeleteResult;
import org.apache.distributedlog.statestore.api.mvcc.result.PutResult;
import org.apache.distributedlog.statestore.api.mvcc.result.RangeResult;
import org.apache.distributedlog.statestore.api.mvcc.result.TxnResult;
import org.apache.distributedlog.statestore.exceptions.InvalidStateStoreException;
import org.apache.distributedlog.statestore.exceptions.MVCCStoreException;
import org.apache.distributedlog.statestore.exceptions.StateStoreClosedException;
import org.apache.distributedlog.statestore.exceptions.StateStoreException;
import org.apache.distributedlog.statestore.exceptions.StateStoreRuntimeException;
import org.apache.distributedlog.statestore.impl.mvcc.op.proto.ProtoPutOpImpl;
import org.apache.distributedlog.statestore.proto.Command;
import org.apache.distributedlog.util.Utils;

/**
 * MVCC Async Store Implementation.
 */
@Slf4j
class MVCCAsyncBytesStoreImpl implements MVCCAsyncStore<byte[], byte[]> {

    // parameters
    private String name = "UNINITIALIZED";

    // local state
    private final MVCCStoreImpl<byte[], byte[]> localStore;
    private boolean ownWriteScheduler = false;
    private boolean ownReadScheduler = false;
    private ScheduledExecutorService writeIOScheduler;
    private ScheduledExecutorService readIOScheduler;

    // journal
    private Namespace logNamespace;
    private DistributedLogManager logManager;
    private AsyncLogWriter writer;
    private long nextRevision;

    // close state
    private boolean isInitialized = false;
    private CompletableFuture<Void> closeFuture = null;

    MVCCAsyncBytesStoreImpl(Supplier<MVCCStoreImpl<byte[], byte[]>> storeSupplier,
                            Supplier<Namespace> namespaceSupplier) {
        this.localStore = storeSupplier.get();
        this.logNamespace = namespaceSupplier.get();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public OpFactory<byte[], byte[]> getOpFactory() {
        return localStore.getOpFactory();
    }

    @VisibleForTesting
    boolean ownWriteScheduler() {
        return ownWriteScheduler;
    }

    @VisibleForTesting
    boolean ownReadScheduler() {
        return ownReadScheduler;
    }

    synchronized AsyncLogWriter getWriter() {
        return writer;
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

    private <T> CompletableFuture<T> executeWriteIO(Callable<T> callable) {
        return executeIO(writeIOScheduler, callable);
    }

    private <T> CompletableFuture<T> executeReadIO(Callable<T> callable) {
        return executeIO(readIOScheduler, callable);
    }

    private CompletableFuture<DLSN> writeCommand(Command command) {
        if (log.isTraceEnabled()) {
            log.trace("Writing command {} to mvcc store {}", command, name());
        }
        ByteBuf recordBuf;
        try {
            recordBuf = MVCCUtils.newLogRecordBuf(command);
        } catch (StateStoreRuntimeException e) {
            return FutureUtils.exception(e);
        }
        synchronized (this) {
            long txId = ++nextRevision;
            return FutureUtils.ensure(
                writer.write(new LogRecord(txId, recordBuf.nioBuffer())),
                () -> recordBuf.release());
        }
    }

    private CompletableFuture<Long> writeCommandReturnTxId(Command command) {
        ByteBuf recordBuf = MVCCUtils.newLogRecordBuf(command);
        synchronized (this) {
            long txId = ++nextRevision;
            return FutureUtils.ensure(
                writer.write(new LogRecord(txId, recordBuf.nioBuffer()))
                    .thenApply(dlsn -> txId),
                () -> recordBuf.release());
        }
    }

    private void validateStoreSpec(StateStoreSpec spec) {
        checkArgument(spec.stream().isPresent(), "No log stream is specified for state store " + spec.name());
    }

    @Override
    public CompletableFuture<Void> init(StateStoreSpec spec) {
        try {
            validateStoreSpec(spec);
        } catch (IllegalArgumentException e) {
            log.error("Fail to init state store due to : ", e);
            return FutureUtils.exception(e);
        }
        name = spec.name();

        if (spec.writeIOScheduler().isPresent()) {
            writeIOScheduler = spec.writeIOScheduler().get();
            ownWriteScheduler = false;
        } else {
            ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("statestore-" + spec.name() + "-write-io-scheduler-%d")
                .build();
            writeIOScheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
            ownWriteScheduler = true;
        }

        if (spec.readIOScheduler().isPresent()) {
            readIOScheduler = spec.readIOScheduler().get();
        } else if (ownWriteScheduler) {
            readIOScheduler = writeIOScheduler;
            ownReadScheduler = false;
        } else {
            ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("statestore-" + spec.name() + "-read-io-scheduler-%d")
                .build();
            readIOScheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
            ownReadScheduler = true;
        }

        return initializeLocalStore(spec)
            .thenComposeAsync(ignored -> initializeJournalWriter(spec), writeIOScheduler)
            .thenComposeAsync(endDLSN -> {
                log.info("Successfully write a barrier record for mvcc store {} at {}", name, endDLSN);
                return replayJournal(endDLSN);
            }, writeIOScheduler);
    }

    private CompletableFuture<Void> initializeLocalStore(StateStoreSpec spec) {
        return executeWriteIO(() -> {
            log.info("Initializing the local state for mvcc store {}", name());
            localStore.init(spec);
            log.info("Initialized the local state for mvcc store {}", name());
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
            logManager = logNamespace.openLog(spec.stream().get());
        } catch (IOException e) {
            return FutureUtils.exception(e);
        }
        return logManager.openAsyncLogWriter().thenComposeAsync(w -> {
            synchronized (this) {
                writer = w;
                nextRevision = writer.getLastTxId();
                if (nextRevision < 0) {
                    nextRevision = 0L;
                }
                log.info("Initialized the journal writer for mvcc store {} : last revision = {}",
                    name(), nextRevision);
            }
            return writeCommand(NOP_CMD);
        }, writeIOScheduler);
    }

    private CompletableFuture<Void> replayJournal(DLSN endDLSN) {
        return logManager.openAsyncLogReader(DLSN.InitialDLSN)
            .thenComposeAsync(r -> {
                CompletableFuture<Void> replayFuture = FutureUtils.createFuture();
                FutureUtils.ensure(replayFuture, () -> r.asyncClose());

                log.info("Successfully open the journal reader for mvcc store {}", name());
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
                    log.debug("Received command record {} to replay at mvcc store {}",
                        record, name());
                }
                if (record.getDlsn().compareTo(endDLSN) >= 0) {
                    log.info("Finished replaying journal for state store {}", name());
                    markInitialized();
                    FutureUtils.complete(future, null);
                    return;
                }

                try {
                    applyCommand(record);
                    // read next record
                    replayJournal(reader, endDLSN, future);
                } catch (StateStoreRuntimeException e) {
                    FutureUtils.completeExceptionally(future, e);
                }
            }

            @Override
            public void onFailure(Throwable cause) {
                FutureUtils.completeExceptionally(future, cause);
            }
        });
    }

    private void applyCommand(LogRecordWithDLSN record) {
        Command command = newCommand(record.getPayloadBuf());
        switch (command.getReqCase()) {
            case NOP_REQ:
                return;
            case PUT_REQ:
                applyPutCommand(record.getTransactionId(), command);
                return;
            case DELETE_REQ:
                applyDeleteCommand(record.getTransactionId(), command);
                return;
            case TXN_REQ:
                applyTxnCommand(record.getTransactionId(), command);
                return;
            default:
                return;
        }
    }

    private void applyPutCommand(long revision, Command command) {
        ProtoPutOpImpl op = ProtoPutOpImpl.newPutOp(revision, command);
        try {
            applyPutOp(revision, op, true);
        } finally {
            op.recycle();
        }
    }

    private void applyPutOp(long revision,
                            PutOp<byte[], byte[]> op,
                            boolean ignoreSmallerRevision) {
        PutResult<byte[], byte[]> result = localStore.put(revision, op);
        try {
            if (Code.OK == result.code()
                || (ignoreSmallerRevision && Code.SMALLER_REVISION == result.code())) {
                return;
            }
            throw new MVCCStoreException(result.code(),
                "Failed to apply command " + op + " at revision "
                    + revision + " to the state store " + name());
        } finally {
            result.recycle();
        }
    }

    private void applyDeleteCommand(long revision, Command command) {
        throw new UnsupportedOperationException();
    }

    private void applyTxnCommand(long revision, Command command) {
        throw new UnsupportedOperationException();
    }

    private synchronized void markInitialized() {
        isInitialized = true;
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

        FutureUtils.ensure(
            // close the log streams
            Utils.closeSequence(
                writeIOScheduler,
                true,
                getWriter(),
                logManager
            ),
            // close the local state store
            () -> {
                if (null == writeIOScheduler) {
                    closeLocalStore();
                    FutureUtils.complete(future, null);
                    return;
                }
                writeIOScheduler.submit(() -> {
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
            log.warn("Fail to flush local state store " + name(), e);
        }
        // close the local store
        localStore.close();
    }

    @Override
    public CompletableFuture<RangeResult<byte[], byte[]>> range(RangeOp<byte[], byte[]> rangeOp) {
        synchronized (this) {
            if (!isInitialized) {
                return FutureUtils.exception(new InvalidStateStoreException("State store is not initialized yet."));
            }
        }

        // consider running read operations in separate I/O threads
        return executeReadIO(() -> localStore.range(rangeOp));
    }

    @Override
    public CompletableFuture<PutResult<byte[], byte[]>> put(PutOp<byte[], byte[]> op) {
        synchronized (this) {
            if (!isInitialized) {
                return FutureUtils.exception(new InvalidStateStoreException("State store is not initialized yet."));
            }
        }

        Command command = toCommand(op);
        return writeCommandReturnTxId(command)
            .thenApplyAsync(revision -> localStore.put(revision, op), writeIOScheduler);
    }

    @Override
    public CompletableFuture<DeleteResult<byte[], byte[]>> delete(DeleteOp<byte[], byte[]> op) {
        synchronized (this) {
            if (!isInitialized) {
                return FutureUtils.exception(new InvalidStateStoreException("State store is not initialized yet."));
            }
        }

        Command command = toCommand(op);
        return writeCommandReturnTxId(command)
            .thenApplyAsync(revision -> localStore.delete(revision, op), writeIOScheduler);
    }

    @Override
    public CompletableFuture<TxnResult<byte[], byte[]>> txn(TxnOp<byte[], byte[]> op) {
        synchronized (this) {
            if (!isInitialized) {
                return FutureUtils.exception(new InvalidStateStoreException("State store is not initialized yet."));
            }
        }

        Command command = toCommand(op);
        return writeCommandReturnTxId(command)
            .thenApplyAsync(revision -> localStore.txn(revision, op), writeIOScheduler);
    }
}
