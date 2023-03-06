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
package org.apache.distributedlog;

import static org.apache.distributedlog.EnvelopedEntry.COMPRESSED_SIZE_OFFSET;
import static org.apache.distributedlog.EnvelopedEntry.COMPRESSION_CODEC_MASK;
import static org.apache.distributedlog.EnvelopedEntry.CURRENT_VERSION;
import static org.apache.distributedlog.EnvelopedEntry.DECOMPRESSED_SIZE_OFFSET;
import static org.apache.distributedlog.EnvelopedEntry.FLAGS_OFFSET;
import static org.apache.distributedlog.EnvelopedEntry.HEADER_LENGTH;
import static org.apache.distributedlog.EnvelopedEntry.VERSION_OFFSET;
import static org.apache.distributedlog.LogRecord.MAX_LOGRECORDSET_SIZE;
import static org.apache.distributedlog.LogRecord.MAX_LOGRECORD_SIZE;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.Entry.Writer;
import org.apache.distributedlog.exceptions.InvalidEnvelopedEntryException;
import org.apache.distributedlog.exceptions.LogRecordTooLongException;
import org.apache.distributedlog.exceptions.WriteException;
import org.apache.distributedlog.io.CompressionCodec;
import org.apache.distributedlog.io.CompressionCodec.Type;
import org.apache.distributedlog.io.CompressionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ByteBuf} based log record set writer.
 */
class EnvelopedEntryWriter implements Writer {

    private static final Logger logger = LoggerFactory.getLogger(EnvelopedEntryWriter.class);

    private static class WriteRequest {

        private final int numRecords;
        private final CompletableFuture<DLSN> promise;

        WriteRequest(int numRecords, CompletableFuture<DLSN> promise) {
            this.numRecords = numRecords;
            this.promise = promise;
        }

    }

    private final String logName;
    private final ByteBuf buffer;
    private ByteBuf finalizedBuffer = null;
    private final LogRecord.Writer writer;
    private final List<WriteRequest> writeRequests;
    private final boolean envelopeBeforeTransmit;
    private final CompressionCodec.Type codec;
    private final int flags;
    private int count = 0;
    private boolean hasUserData = false;
    private long maxTxId = Long.MIN_VALUE;

    EnvelopedEntryWriter(String logName,
                         int initialBufferSize,
                         boolean envelopeBeforeTransmit,
                         CompressionCodec.Type codec) {
        this.logName = logName;
        this.buffer = PooledByteBufAllocator.DEFAULT.buffer(
                Math.min(Math.max(initialBufferSize * 6 / 5, HEADER_LENGTH), MAX_LOGRECORDSET_SIZE),
                MAX_LOGRECORDSET_SIZE);
        this.writer = new LogRecord.Writer(buffer);
        this.writeRequests = new LinkedList<WriteRequest>();
        this.envelopeBeforeTransmit = envelopeBeforeTransmit;
        this.codec = codec;
        this.flags = codec.code() & COMPRESSION_CODEC_MASK;
        if (envelopeBeforeTransmit) {
            this.buffer.writerIndex(HEADER_LENGTH);
        }
    }

    @Override
    public synchronized void writeRecord(LogRecord record,
                                         CompletableFuture<DLSN> transmitPromise)
            throws LogRecordTooLongException, WriteException {
        int logRecordSize = record.getPersistentSize();
        if (logRecordSize > MAX_LOGRECORD_SIZE) {
            throw new LogRecordTooLongException(
                    "Log Record of size " + logRecordSize + " written when only "
                            + MAX_LOGRECORD_SIZE + " is allowed");
        }

        try {
            this.writer.writeOp(record);
            int numRecords = 1;
            if (!record.isControl()) {
                hasUserData = true;
            }
            if (record.isRecordSet()) {
                numRecords = LogRecordSet.numRecords(record);
            }
            count += numRecords;
            writeRequests.add(new WriteRequest(numRecords, transmitPromise));
            maxTxId = Math.max(maxTxId, record.getTransactionId());
        } catch (IOException e) {
            logger.error("Failed to append record to record set of {} : ",
                    logName, e);
            throw new WriteException(logName, "Failed to append record to record set of "
                    + logName);
        }
    }

    private synchronized void satisfyPromises(long lssn, long entryId) {
        long nextSlotId = 0;
        for (WriteRequest request : writeRequests) {
            request.promise.complete(new DLSN(lssn, entryId, nextSlotId));
            nextSlotId += request.numRecords;
        }
        writeRequests.clear();
    }

    private synchronized void cancelPromises(Throwable reason) {
        for (WriteRequest request : writeRequests) {
            request.promise.completeExceptionally(reason);
        }
        writeRequests.clear();
    }

    @Override
    public synchronized long getMaxTxId() {
        return maxTxId;
    }

    @Override
    public synchronized boolean hasUserRecords() {
        return hasUserData;
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
    public synchronized ByteBuf getBuffer() throws InvalidEnvelopedEntryException, IOException {
        if (null == finalizedBuffer) {
            finalizedBuffer = finalizeBuffer();
        }
        return finalizedBuffer.retainedSlice();
    }

    private ByteBuf finalizeBuffer() {
       if (!envelopeBeforeTransmit) {
            return buffer.retain();
        }

        int dataOffset = HEADER_LENGTH;
        int dataLen = buffer.readableBytes() - HEADER_LENGTH;

        if (Type.NONE == codec) {
            // update version
            buffer.setByte(VERSION_OFFSET, CURRENT_VERSION);
            // update the flags
            buffer.setInt(FLAGS_OFFSET, flags);
            // update data len
            buffer.setInt(DECOMPRESSED_SIZE_OFFSET, dataLen);
            buffer.setInt(COMPRESSED_SIZE_OFFSET, dataLen);
            return buffer.retain();
        }

        // compression
        CompressionCodec compressor =
                CompressionUtils.getCompressionCodec(codec);
        ByteBuf uncompressedBuf = buffer.slice(dataOffset, dataLen);
        ByteBuf compressedBuf = compressor.compress(uncompressedBuf, HEADER_LENGTH);
        // update version
        compressedBuf.setByte(VERSION_OFFSET, CURRENT_VERSION);
        // update the flags
        compressedBuf.setInt(FLAGS_OFFSET, flags);
        // update data len
        compressedBuf.setInt(DECOMPRESSED_SIZE_OFFSET, dataLen);
        compressedBuf.setInt(COMPRESSED_SIZE_OFFSET, compressedBuf.readableBytes() - HEADER_LENGTH);
        return compressedBuf;
    }

    @Override
    public synchronized DLSN finalizeTransmit(long lssn, long entryId) {
        return new DLSN(lssn, entryId, count - 1);
    }

    @Override
    public void completeTransmit(long lssn, long entryId) {
        satisfyPromises(lssn, entryId);
        ReferenceCountUtil.release(buffer);
        synchronized (this) {
            ReferenceCountUtil.release(finalizedBuffer);
        }
    }

    @Override
    public void abortTransmit(Throwable reason) {
        cancelPromises(reason);
        ReferenceCountUtil.release(buffer);
        synchronized (this) {
            ReferenceCountUtil.release(finalizedBuffer);
        }
    }
}
