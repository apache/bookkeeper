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

import static org.apache.distributedlog.LogRecordSet.METADATA_COMPRESSION_MASK;
import static org.apache.distributedlog.LogRecordSet.METADATA_VERSION_MASK;
import static org.apache.distributedlog.LogRecordSet.VERSION;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.io.CompressionCodec;
import org.apache.distributedlog.io.CompressionCodec.Type;
import org.apache.distributedlog.io.CompressionUtils;

/**
 * Record reader to read records from an enveloped entry buffer.
 */
@Slf4j
class EnvelopedRecordSetReader implements LogRecordSet.Reader {

    private final long logSegmentSeqNo;
    private final long entryId;
    private final long transactionId;
    private final long startSequenceId;
    private int numRecords;
    private final ByteBuf reader;

    // slot id
    private long slotId;
    private int position;

    EnvelopedRecordSetReader(long logSegmentSeqNo,
                             long entryId,
                             long transactionId,
                             long startSlotId,
                             int startPositionWithinLogSegment,
                             long startSequenceId,
                             ByteBuf src)
            throws IOException {
        this.logSegmentSeqNo = logSegmentSeqNo;
        this.entryId = entryId;
        this.transactionId = transactionId;
        this.slotId = startSlotId;
        this.position = startPositionWithinLogSegment;
        this.startSequenceId = startSequenceId;

        // read data
        int metadata = src.readInt();
        int version = metadata & METADATA_VERSION_MASK;
        if (version != VERSION) {
            throw new IOException(String.format("Version mismatch while reading. Received: %d,"
                + " Required: %d", version, VERSION));
        }
        int codecCode = metadata & METADATA_COMPRESSION_MASK;
        this.numRecords = src.readInt();
        int decompressedDataLen = src.readInt();
        int compressedDataLen = src.readInt();
        ByteBuf compressedBuf = src.slice(src.readerIndex(), compressedDataLen);
        try {
            if (Type.NONE.code() == codecCode && decompressedDataLen != compressedDataLen) {
                throw new IOException("Inconsistent data length found for a non-compressed record set : decompressed = "
                        + decompressedDataLen + ", actual = " + compressedDataLen);
            }
            CompressionCodec codec = CompressionUtils.getCompressionCodec(Type.of(codecCode));
            this.reader = codec.decompress(compressedBuf, decompressedDataLen);
        } finally {
            compressedBuf.release();
        }
        if (numRecords == 0) {
            this.reader.release();
        }
    }

    @Override
    public LogRecordWithDLSN nextRecord() throws IOException {
        if (numRecords <= 0) {
            return null;
        }

        int recordLen = reader.readInt();
        ByteBuf recordBuf = reader.slice(reader.readerIndex(), recordLen);
        reader.readerIndex(reader.readerIndex() + recordLen);

        DLSN dlsn = new DLSN(logSegmentSeqNo, entryId, slotId);
        LogRecordWithDLSN record =
                new LogRecordWithDLSN(dlsn, startSequenceId);
        record.setPositionWithinLogSegment(position);
        record.setTransactionId(transactionId);
        record.setPayloadBuf(recordBuf, true);

        ++slotId;
        ++position;
        --numRecords;

        // release the record set buffer when exhausting the reader
        if (0 == numRecords) {
            this.reader.release();
        }

        return record;
    }

    @Override
    public void release() {
        if (0 != numRecords) {
            numRecords = 0;
            reader.release();
        }
    }
}
