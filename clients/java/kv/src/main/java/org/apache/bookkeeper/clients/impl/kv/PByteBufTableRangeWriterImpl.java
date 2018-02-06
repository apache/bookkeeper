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
package org.apache.bookkeeper.clients.impl.kv;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;

import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.kv.PTableWriter;
import org.apache.bookkeeper.api.kv.exceptions.KvApiException;
import org.apache.bookkeeper.api.kv.result.Code;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.store.Command;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;

/**
 * A table writer that write events to a given table range.
 */
@Slf4j
class PByteBufTableRangeWriterImpl implements PTableWriter<ByteBuf, ByteBuf> {

    // TODO: it is duplicated from `TableStoreUtils`. consolidate to some common places
    static final byte HAS_ROUTING_KEY = 0x1;
    static final byte NO_ROUTING_KEY = 0x0;
    static final byte SEP = 0x0;

    static boolean hasRKey(ByteBuf rKey) {
        return null != rKey && rKey.readableBytes() > 0;
    }

    static ByteBuf newStoreKey(ByteBuf rKey, ByteBuf lKey) {
        boolean hasRkey = hasRKey(rKey);

        int keyLen;
        if (hasRkey) {
            keyLen = rKey.readableBytes() + lKey.readableBytes() + 2;
        } else {
            keyLen = lKey.readableBytes() + 1;
        }
        ByteBuf keyBuf = PooledByteBufAllocator.DEFAULT.buffer(keyLen);
        if (hasRkey) {
            keyBuf.writeByte(HAS_ROUTING_KEY);
            keyBuf.writeBytes(rKey);
            keyBuf.writeByte(SEP);
            keyBuf.writeBytes(lKey);
        } else {
            keyBuf.writeByte(NO_ROUTING_KEY);
            keyBuf.writeBytes(lKey);
        }

        return keyBuf;
    }

    static String streamName(long scId,
                             long streamId,
                             long rangeId) {
        // TODO: change to filesystem path
        return String.format(
            "%s_%018d_%018d_%018d",
            "streams",
            scId,
            streamId,
            rangeId);
    }

    private final DistributedLogManager dlm;
    private final CompletableFuture<AsyncLogWriter> logWriterFuture;
    private long nextRevision;

    PByteBufTableRangeWriterImpl(StreamProperties streamProps,
                                 RangeProperties rangeProps,
                                 Namespace namespace) throws IOException {
        this.dlm = namespace.openLog(streamName(
            rangeProps.getStorageContainerId(),
            streamProps.getStreamId(),
            rangeProps.getRangeId()));
        this.logWriterFuture = dlm.openAsyncLogWriter()
            .thenApply(writer -> {
                synchronized (PByteBufTableRangeWriterImpl.this) {
                    nextRevision = writer.getLastTxId();
                    if (nextRevision < 0L) {
                        nextRevision = 0L;
                    }
                }
                return writer;
            });
    }

    @Override
    public CompletableFuture<Void> write(long sequenceId, ByteBuf pKey, ByteBuf lKey, ByteBuf value) {
        pKey.retain();
        lKey.retain();
        if (null != value) {
            value.retain();
        }
        ByteBuf storeKey = newStoreKey(pKey.slice(), lKey.slice());
        Command command;
        if (null != value) {
            command = Command.newBuilder()
                .setPutReq(PutRequest.newBuilder()
                    .setKey(UnsafeByteOperations.unsafeWrap(storeKey.nioBuffer()))
                    .setValue(UnsafeByteOperations.unsafeWrap(value.nioBuffer())))
                .build();
        } else {
            command = Command.newBuilder()
                .setDeleteReq(DeleteRangeRequest.newBuilder()
                    .setKey(UnsafeByteOperations.unsafeWrap(storeKey.nioBuffer()))
                    .build())
                .build();
        }
        ByteBuf recordBuf = PooledByteBufAllocator.DEFAULT.buffer(command.getSerializedSize());
        try {
            command.writeTo(new ByteBufOutputStream(recordBuf));
        } catch (IOException ioe) {
            pKey.release();
            lKey.release();
            if (null != value) {
                value.release();
            }
            storeKey.release();
            recordBuf.release();

            return FutureUtils.exception(new KvApiException(Code.UNEXPECTED, "Invalid command : " + command));
        }

        return writeCommandBuf(recordBuf)
            .whenComplete((v, cause) -> {
                pKey.release();
                lKey.release();
                if (null != value) {
                    value.release();
                }
                storeKey.release();
                recordBuf.release();
            });
    }

    @Override
    public CompletableFuture<Void> increment(long sequenceId, ByteBuf pKey, ByteBuf lKey, long amount) {
        pKey.retain();
        lKey.retain();
        ByteBuf storeKey = newStoreKey(pKey.slice(), lKey.slice());
        Command command = Command.newBuilder()
            .setIncrReq(IncrementRequest.newBuilder()
                .setKey(UnsafeByteOperations.unsafeWrap(storeKey.nioBuffer()))
                .setAmount(amount))
            .build();
        ByteBuf recordBuf = PooledByteBufAllocator.DEFAULT.buffer(command.getSerializedSize());
        try {
            command.writeTo(new ByteBufOutputStream(recordBuf));
        } catch (IOException ioe) {
            pKey.release();
            lKey.release();
            storeKey.release();
            recordBuf.release();

            return FutureUtils.exception(new KvApiException(Code.UNEXPECTED, "Invalid command : " + command));
        }
        return writeCommandBuf(recordBuf)
            .whenComplete((v, cause) -> {
                pKey.release();
                lKey.release();
                storeKey.release();
                recordBuf.release();
            });
    }

    private CompletableFuture<Void> writeCommandBuf(ByteBuf recordBuf) {
        return logWriterFuture.thenCompose(writer -> {
            long txId;
            synchronized (PByteBufTableRangeWriterImpl.this) {
                txId = ++nextRevision;
                return writer.write(new LogRecord(txId, recordBuf.nioBuffer()));
            }
        }).thenApply(dlsn -> null);
    }

    @Override
    public void close() {
        logWriterFuture.completeExceptionally(
            new Exception("Cancel the log writer"));
        try {
            AsyncLogWriter logWriter = result(logWriterFuture);
            result(logWriter.asyncClose());
        } catch (Exception e) {
            // ignore the error
        }
        try {
            dlm.close();
        } catch (IOException e) {
            log.warn("Failed to close range writer {}", dlm.getStreamName());
        }
    }
}
