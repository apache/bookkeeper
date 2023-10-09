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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.distributedlog.EnvelopedEntry.HEADER_LENGTH;
import static org.apache.distributedlog.LogRecord.MAX_LOGRECORD_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.Entry.Reader;
import org.apache.distributedlog.Entry.Writer;
import org.apache.distributedlog.exceptions.LogRecordTooLongException;
import org.apache.distributedlog.io.CompressionCodec;
import org.apache.distributedlog.util.Utils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test Case of {@link Entry}.
 */
public class TestEntry {

    @Test(timeout = 20000)
    public void testEmptyRecordSet() throws Exception {
        Writer writer = Entry.newEntry(
                "test-empty-record-set",
                1024,
                true,
                CompressionCodec.Type.NONE);
        assertEquals("zero bytes", HEADER_LENGTH, writer.getNumBytes());
        assertEquals("zero records", 0, writer.getNumRecords());

        ByteBuf buffer = writer.getBuffer();
        EnvelopedEntryReader reader = (EnvelopedEntryReader) Entry.newBuilder()
                .setEntry(buffer)
                .setLogSegmentInfo(1L, 0L)
                .setEntryId(0L)
                .buildReader();
        int refCnt = reader.getSrcBuf().refCnt();
        assertFalse(reader.isExhausted());
        Assert.assertNull("Empty record set should return null",
                reader.nextRecord());
        assertTrue(reader.isExhausted());
        assertEquals(refCnt - 1, reader.getSrcBuf().refCnt());

        // read next record again (to test release buffer)
        Assert.assertNull("Empty record set should return null",
            reader.nextRecord());
        assertEquals(refCnt - 1, reader.getSrcBuf().refCnt());
        ReferenceCountUtil.release(buffer);
    }

    @Test(timeout = 20000)
    public void testWriteTooLongRecord() throws Exception {
        Writer writer = Entry.newEntry(
                "test-write-too-long-record",
                1024,
                true,
                CompressionCodec.Type.NONE);
        assertEquals("zero bytes", HEADER_LENGTH, writer.getNumBytes());
        assertEquals("zero records", 0, writer.getNumRecords());

        LogRecord largeRecord = new LogRecord(1L, new byte[MAX_LOGRECORD_SIZE + 1]);
        try {
            writer.writeRecord(largeRecord, new CompletableFuture<DLSN>());
            Assert.fail("Should fail on writing large record");
        } catch (LogRecordTooLongException lrtle) {
            // expected
        }
        assertEquals("zero bytes", HEADER_LENGTH, writer.getNumBytes());
        assertEquals("zero records", 0, writer.getNumRecords());

        ByteBuf buffer = writer.getBuffer();
        assertEquals("zero bytes", HEADER_LENGTH, buffer.readableBytes());
        ReferenceCountUtil.release(buffer);
    }

    @Test(timeout = 20000)
    public void testWriteRecords() throws Exception {
        Writer writer = Entry.newEntry(
                "test-write-records",
                1024,
                true,
                CompressionCodec.Type.NONE);
        assertEquals("zero bytes", HEADER_LENGTH, writer.getNumBytes());
        assertEquals("zero records", 0, writer.getNumRecords());

        List<CompletableFuture<DLSN>> writePromiseList = Lists.newArrayList();
        // write first 5 records
        for (int i = 0; i < 5; i++) {
            LogRecord record = new LogRecord(i, ("record-" + i).getBytes(UTF_8));
            record.setPositionWithinLogSegment(i);
            CompletableFuture<DLSN> writePromise = new CompletableFuture<DLSN>();
            writer.writeRecord(record, writePromise);
            writePromiseList.add(writePromise);
            assertEquals((i + 1) + " records", (i + 1), writer.getNumRecords());
        }

        // write large record
        LogRecord largeRecord = new LogRecord(1L, new byte[MAX_LOGRECORD_SIZE + 1]);
        try {
            writer.writeRecord(largeRecord, new CompletableFuture<DLSN>());
            Assert.fail("Should fail on writing large record");
        } catch (LogRecordTooLongException lrtle) {
            // expected
        }
        assertEquals("5 records", 5, writer.getNumRecords());

        // write another 5 records
        for (int i = 0; i < 5; i++) {
            LogRecord record = new LogRecord(i + 5, ("record-" + (i + 5)).getBytes(UTF_8));
            record.setPositionWithinLogSegment(i + 5);
            CompletableFuture<DLSN> writePromise = new CompletableFuture<DLSN>();
            writer.writeRecord(record, writePromise);
            writePromiseList.add(writePromise);
            assertEquals((i + 6) + " records", (i + 6), writer.getNumRecords());
        }

        ByteBuf buffer = writer.getBuffer();
        buffer.retain();

        // Test transmit complete
        writer.completeTransmit(1L, 1L);
        List<DLSN> writeResults = Utils.ioResult(FutureUtils.collect(writePromiseList));
        for (int i = 0; i < 10; i++) {
            assertEquals(new DLSN(1L, 1L, i), writeResults.get(i));
        }

        // Test reading from buffer
        Reader reader = Entry.newBuilder()
                .setEntry(buffer)
                .setLogSegmentInfo(1L, 1L)
                .setEntryId(0L)
                .setEnvelopeEntry(true)
                .buildReader();
        ReferenceCountUtil.release(buffer);
        LogRecordWithDLSN record = reader.nextRecord();
        int numReads = 0;
        long expectedTxid = 0L;
        while (null != record) {
            assertEquals(expectedTxid, record.getTransactionId());
            assertEquals(expectedTxid, record.getSequenceId());
            assertEquals(new DLSN(1L, 0L, expectedTxid), record.getDlsn());
            ++numReads;
            ++expectedTxid;
            record = reader.nextRecord();
        }
        assertEquals(10, numReads);

        reader.release();
    }

    @Test(timeout = 20000)
    public void testWriteRecordSet() throws Exception {
        Writer writer = Entry.newEntry(
                "test-write-recordset",
                1024,
                true,
                CompressionCodec.Type.NONE);
        assertEquals("zero bytes", HEADER_LENGTH, writer.getNumBytes());
        assertEquals("zero records", 0, writer.getNumRecords());

        List<CompletableFuture<DLSN>> writePromiseList = Lists.newArrayList();
        // write first 5 records
        for (int i = 0; i < 5; i++) {
            LogRecord record = new LogRecord(i, ("record-" + i).getBytes(UTF_8));
            record.setPositionWithinLogSegment(i);
            CompletableFuture<DLSN> writePromise = new CompletableFuture<DLSN>();
            writer.writeRecord(record, writePromise);
            writePromiseList.add(writePromise);
            assertEquals((i + 1) + " records", (i + 1), writer.getNumRecords());
        }

        final LogRecordSet.Writer recordSetWriter = LogRecordSet.newWriter(1024, CompressionCodec.Type.NONE);
        List<CompletableFuture<DLSN>> recordSetPromiseList = Lists.newArrayList();
        // write another 5 records as a batch
        for (int i = 0; i < 5; i++) {
            ByteBuffer record = ByteBuffer.wrap(("record-" + (i + 5)).getBytes(UTF_8));
            CompletableFuture<DLSN> writePromise = new CompletableFuture<DLSN>();
            recordSetWriter.writeRecord(record, writePromise);
            recordSetPromiseList.add(writePromise);
            assertEquals((i + 1) + " records", (i + 1), recordSetWriter.getNumRecords());
        }
        final ByteBuf recordSetBuffer = recordSetWriter.getBuffer();
        LogRecord setRecord = new LogRecord(5L, recordSetBuffer);
        setRecord.setPositionWithinLogSegment(5);
        setRecord.setRecordSet();
        CompletableFuture<DLSN> writePromise = new CompletableFuture<DLSN>();
        writePromise.whenComplete(new FutureEventListener<DLSN>() {
            @Override
            public void onSuccess(DLSN dlsn) {
                recordSetWriter.completeTransmit(
                        dlsn.getLogSegmentSequenceNo(),
                        dlsn.getEntryId(),
                        dlsn.getSlotId());
            }

            @Override
            public void onFailure(Throwable cause) {
                recordSetWriter.abortTransmit(cause);
            }
        });
        writer.writeRecord(setRecord, writePromise);
        writePromiseList.add(writePromise);

        // write last 5 records
        for (int i = 0; i < 5; i++) {
            LogRecord record = new LogRecord(i + 10, ("record-" + (i + 10)).getBytes(UTF_8));
            record.setPositionWithinLogSegment(i + 10);
            writePromise = new CompletableFuture<DLSN>();
            writer.writeRecord(record, writePromise);
            writePromiseList.add(writePromise);
            assertEquals((i + 11) + " records", (i + 11), writer.getNumRecords());
        }

        ByteBuf buffer = writer.getBuffer();
        buffer.retain();

        // Test transmit complete
        writer.completeTransmit(1L, 1L);
        List<DLSN> writeResults = Utils.ioResult(FutureUtils.collect(writePromiseList));
        for (int i = 0; i < 5; i++) {
            assertEquals(new DLSN(1L, 1L, i), writeResults.get(i));
        }
        assertEquals(new DLSN(1L, 1L, 5), writeResults.get(5));
        for (int i = 0; i < 5; i++) {
            assertEquals(new DLSN(1L, 1L, (10 + i)), writeResults.get(6 + i));
        }
        List<DLSN> recordSetWriteResults = Utils.ioResult(FutureUtils.collect(recordSetPromiseList));
        for (int i = 0; i < 5; i++) {
            assertEquals(new DLSN(1L, 1L, (5 + i)), recordSetWriteResults.get(i));
        }

        // Test reading from buffer
        verifyReadResult(buffer, 1L, 1L, 1L, true,
                new DLSN(1L, 1L, 2L), 3, 5, 5,
                new DLSN(1L, 1L, 2L), 2L);
        verifyReadResult(buffer, 1L, 1L, 1L, true,
                new DLSN(1L, 1L, 7L), 0, 3, 5,
                new DLSN(1L, 1L, 7L), 7L);
        verifyReadResult(buffer, 1L, 1L, 1L, true,
                new DLSN(1L, 1L, 12L), 0, 0, 3,
                new DLSN(1L, 1L, 12L), 12L);
        verifyReadResult(buffer, 1L, 1L, 1L, false,
                new DLSN(1L, 1L, 2L), 3, 5, 5,
                new DLSN(1L, 1L, 2L), 2L);
        verifyReadResult(buffer, 1L, 1L, 1L, false,
                new DLSN(1L, 1L, 7L), 0, 3, 5,
                new DLSN(1L, 1L, 7L), 7L);
        verifyReadResult(buffer, 1L, 1L, 1L, false,
                new DLSN(1L, 1L, 12L), 0, 0, 3,
                new DLSN(1L, 1L, 12L), 12L);

        ReferenceCountUtil.release(buffer);
    }

    void verifyReadResult(ByteBuf data,
                          long lssn, long entryId, long startSequenceId,
                          boolean deserializeRecordSet,
                          DLSN skipTo,
                          int firstNumRecords,
                          int secondNumRecords,
                          int lastNumRecords,
                          DLSN expectedDLSN,
                          long expectedTxId) throws Exception {
        Reader reader = Entry.newBuilder()
                .setEntry(data)
                .setLogSegmentInfo(lssn, startSequenceId)
                .setEntryId(entryId)
                .deserializeRecordSet(deserializeRecordSet)
                .buildReader();
        reader.skipTo(skipTo);

        LogRecordWithDLSN record;
        for (int i = 0; i < firstNumRecords; i++) { // first
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals(expectedDLSN, record.getDlsn());
            assertEquals(expectedTxId, record.getTransactionId());
            assertNotNull("record " + record + " payload is null",
                    record.getPayloadBuf());
            assertEquals("record-" + expectedTxId, new String(record.getPayload(), UTF_8));
            expectedDLSN = expectedDLSN.getNextDLSN();
            ++expectedTxId;
        }

        boolean verifyDeserializedRecords = true;
        if (firstNumRecords > 0) {
            verifyDeserializedRecords = deserializeRecordSet;
        }
        if (verifyDeserializedRecords) {
            long txIdOfRecordSet = 5;
            for (int i = 0; i < secondNumRecords; i++) {
                record = reader.nextRecord();
                assertNotNull(record);
                assertEquals(expectedDLSN, record.getDlsn());
                assertEquals(txIdOfRecordSet, record.getTransactionId());
                assertNotNull("record " + record + " payload is null",
                        record.getPayload());
                assertEquals("record-" + expectedTxId, new String(record.getPayload(), UTF_8));
                expectedDLSN = expectedDLSN.getNextDLSN();
                ++expectedTxId;
            }
        } else {
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals(expectedDLSN, record.getDlsn());
            assertEquals(expectedTxId, record.getTransactionId());
            for (int i = 0; i < secondNumRecords; i++) {
                expectedDLSN = expectedDLSN.getNextDLSN();
                ++expectedTxId;
            }
        }

        for (int i = 0; i < lastNumRecords; i++) {
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals(expectedDLSN, record.getDlsn());
            assertEquals(expectedTxId, record.getTransactionId());
            assertNotNull("record " + record + " payload is null",
                    record.getPayload());
            assertEquals("record-" + expectedTxId, new String(record.getPayload(), UTF_8));
            expectedDLSN = expectedDLSN.getNextDLSN();
            ++expectedTxId;
        }

    }


}
