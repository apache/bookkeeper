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

import static com.google.common.base.Preconditions.checkArgument;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.exceptions.LogRecordTooLongException;
import org.apache.distributedlog.exceptions.WriteException;
import org.apache.distributedlog.io.CompressionCodec;

/**
 * A set of {@link LogRecord}s.
 *
 * <pre>
 * Structure:
 * Bytes 0  -  4                : Metadata (version + flags)
 * Bytes 4  - 7                 : Number of Records
 * Bytes 8  - 11                : Original Payload Length
 * Bytes 12 - 15                : Actual Payload Length
 * Bytes 16 - 16+length-1       : Payload
 * ------------------------------------------------------
 *
 * Metadata: Version and Flags // 32 Bits
 * --------------------------------------
 * 0 ... 0 0 0 0 0
 *             |_|
 *              |
 *    Compression Codec: // 2 Bits (Least significant)
 *    -----------------
 *    00        : No Compression
 *    01        : LZ4 Compression
 *    10        : Unused
 *    11        : Unused
 *
 * </pre>
 */
public class LogRecordSet {

    public static final int HEADER_LEN =
            Integer.BYTES /* Metadata */
          + Integer.BYTES /* Count */
          + Integer.BYTES + Integer.BYTES /* Lengths: (decompressed + compressed) */
            ;

    // Version
    static final int VERSION = 0x1000;

    static final int METADATA_OFFSET = 0;
    static final int COUNT_OFFSET = METADATA_OFFSET + Integer.BYTES;
    static final int DECOMPRESSED_SIZE_OFFSET = COUNT_OFFSET + Integer.BYTES;
    static final int COMPRESSED_SIZE_OFFSET = DECOMPRESSED_SIZE_OFFSET + Integer.BYTES;

    // Metadata
    static final int METADATA_VERSION_MASK = 0xf000;
    static final int METADATA_COMPRESSION_MASK = 0x3;

    public static int numRecords(LogRecord record) throws IOException {
        checkArgument(record.isRecordSet(),
                "record is not a recordset");
        ByteBuf buffer = record.getPayloadBuf();
        int metadata = buffer.getInt(METADATA_OFFSET);
        int version = (metadata & METADATA_VERSION_MASK);
        if (version != VERSION) {
            throw new IOException(String.format("Version mismatch while reading. Received: %d,"
                + " Required: %d", version, VERSION));
        }
        return buffer.getInt(COUNT_OFFSET);
    }

    public static Writer newWriter(int initialBufferSize,
                                   CompressionCodec.Type codec) {
        return new EnvelopedRecordSetWriter(initialBufferSize, codec);
    }

    public static Reader of(LogRecordWithDLSN record) throws IOException {
        checkArgument(record.isRecordSet(),
                "record is not a recordset");
        DLSN dlsn = record.getDlsn();
        int startPosition = record.getPositionWithinLogSegment();
        long startSequenceId = record.getStartSequenceIdOfCurrentSegment();

        return new EnvelopedRecordSetReader(
                dlsn.getLogSegmentSequenceNo(),
                dlsn.getEntryId(),
                record.getTransactionId(),
                dlsn.getSlotId(),
                startPosition,
                startSequenceId,
                record.getPayloadBuf());
    }

    /**
     * Writer to append {@link LogRecord}s to {@link LogRecordSet}.
     */
    public interface Writer extends LogRecordSetBuffer {

        /**
         * Write a {@link LogRecord} to this record set.
         *
         * @param record
         *          record to write
         * @param transmitPromise
         *          callback for transmit result. the promise is only
         *          satisfied when this record set is transmitted.
         * @throws LogRecordTooLongException if the record is too long
         * @throws WriteException when encountered exception writing the record
         */
        void writeRecord(ByteBuffer record, CompletableFuture<DLSN> transmitPromise)
                throws LogRecordTooLongException, WriteException;
    }

    /**
     * Reader to read {@link LogRecord}s from this record set.
     */
    public interface Reader {

        /**
         * Read next log record from this record set.
         *
         * @return next log record from this record set.
         */
        LogRecordWithDLSN nextRecord() throws IOException;

        /**
         * Release the resources hold by this record set reader.
         */
        void release();

    }

}
