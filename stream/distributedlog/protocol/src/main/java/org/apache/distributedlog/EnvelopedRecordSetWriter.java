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
package org.apache.distributedlog;

import static org.apache.distributedlog.LogRecord.MAX_LOGRECORDSET_SIZE;
import static org.apache.distributedlog.LogRecord.MAX_LOGRECORD_SIZE;
import static org.apache.distributedlog.LogRecordSet.COMPRESSED_SIZE_OFFSET;
import static org.apache.distributedlog.LogRecordSet.COUNT_OFFSET;
import static org.apache.distributedlog.LogRecordSet.DECOMPRESSED_SIZE_OFFSET;
import static org.apache.distributedlog.LogRecordSet.HEADER_LEN;
import static org.apache.distributedlog.LogRecordSet.METADATA_COMPRESSION_MASK;
import static org.apache.distributedlog.LogRecordSet.METADATA_OFFSET;
import static org.apache.distributedlog.LogRecordSet.METADATA_VERSION_MASK;
import static org.apache.distributedlog.LogRecordSet.VERSION;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.exceptions.LogRecordTooLongException;
import org.apache.distributedlog.exceptions.WriteException;
import org.apache.distributedlog.io.CompressionCodec;
import org.apache.distributedlog.io.CompressionCodec.Type;
import org.apache.distributedlog.io.CompressionUtils;

/**
 * {@link ByteBuf} based log record set writer.
 */
@Slf4j
class EnvelopedRecordSetWriter implements LogRecordSet.Writer {

    private final ByteBuf buffer;
    private final List<CompletableFuture<DLSN>> promiseList;
    private final CompressionCodec.Type codec;
    private final int metadata;
    private final int codecCode;
    private int count = 0;
    private ByteBuf recordSetBuffer = null;

    EnvelopedRecordSetWriter(int initialBufferSize,
                             CompressionCodec.Type codec) {
        this.buffer = PooledByteBufAllocator.DEFAULT.buffer(
                Math.max(initialBufferSize, HEADER_LEN),
                MAX_LOGRECORDSET_SIZE);
        this.promiseList = new LinkedList<CompletableFuture<DLSN>>();
        this.codec = codec;
        this.codecCode = codec.code();
        this.metadata = (VERSION & METADATA_VERSION_MASK) | (codecCode & METADATA_COMPRESSION_MASK);
        this.buffer.writeInt(metadata);
        this.buffer.writeInt(0); // count
        this.buffer.writeInt(0); // original len
        this.buffer.writeInt(0); // actual len
    }

    synchronized List<CompletableFuture<DLSN>> getPromiseList() {
        return promiseList;
    }

    @Override
    public synchronized void writeRecord(ByteBuffer record,
                                         CompletableFuture<DLSN> transmitPromise)
            throws LogRecordTooLongException, WriteException {
        int logRecordSize = record.remaining();
        if (logRecordSize > MAX_LOGRECORD_SIZE) {
            throw new LogRecordTooLongException(
                    "Log Record of size " + logRecordSize + " written when only "
                            + MAX_LOGRECORD_SIZE + " is allowed");
        }
        buffer.writeInt(logRecordSize);
        buffer.writeBytes(record);
        ++count;
        promiseList.add(transmitPromise);
    }

    private synchronized void satisfyPromises(long lssn, long entryId, long startSlotId) {
        long nextSlotId = startSlotId;
        for (CompletableFuture<DLSN> promise : promiseList) {
            promise.complete(new DLSN(lssn, entryId, nextSlotId));
            nextSlotId++;
        }
        promiseList.clear();
    }

    private synchronized void cancelPromises(Throwable reason) {
        for (CompletableFuture<DLSN> promise : promiseList) {
            promise.completeExceptionally(reason);
        }
        promiseList.clear();
    }

    @Override
    public int getNumBytes() {
        return buffer.readableBytes();
    }

    @Override
    public synchronized int getNumRecords() {
        return count;
    }

    @Override
    public synchronized ByteBuf getBuffer() {
        if (null == recordSetBuffer) {
            recordSetBuffer = createBuffer();
        }
        return recordSetBuffer.retainedSlice();
    }

    ByteBuf createBuffer() {
        int dataOffset = HEADER_LEN;
        int dataLen = buffer.readableBytes() - HEADER_LEN;

        if (Type.NONE.code() == codecCode) {
            // update count
            buffer.setInt(COUNT_OFFSET, count);
            // update data len
            buffer.setInt(DECOMPRESSED_SIZE_OFFSET, dataLen);
            buffer.setInt(COMPRESSED_SIZE_OFFSET, dataLen);
            return buffer.retain();
        }

        // compression

        CompressionCodec compressor =
                    CompressionUtils.getCompressionCodec(codec);
        ByteBuf uncompressedBuf = buffer.slice(dataOffset, dataLen);
        ByteBuf compressedBuf = compressor.compress(uncompressedBuf, HEADER_LEN);
        compressedBuf.setInt(METADATA_OFFSET, metadata);
        // update count
        compressedBuf.setInt(COUNT_OFFSET, count);
        // update data len
        compressedBuf.setInt(DECOMPRESSED_SIZE_OFFSET, dataLen);
        compressedBuf.setInt(COMPRESSED_SIZE_OFFSET, compressedBuf.readableBytes() - HEADER_LEN);

        return compressedBuf;
    }

    @Override
    public synchronized void completeTransmit(long lssn, long entryId, long startSlotId) {
        satisfyPromises(lssn, entryId, startSlotId);
        buffer.release();
        ReferenceCountUtil.release(recordSetBuffer);
    }

    @Override
    public synchronized void abortTransmit(Throwable reason) {
        cancelPromises(reason);
        buffer.release();
        ReferenceCountUtil.release(recordSetBuffer);
    }
}
