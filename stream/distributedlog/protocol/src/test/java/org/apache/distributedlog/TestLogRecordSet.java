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
import static org.apache.distributedlog.LogRecord.MAX_LOGRECORD_SIZE;
import static org.apache.distributedlog.LogRecordSet.HEADER_LEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.LogRecordSet.Reader;
import org.apache.distributedlog.LogRecordSet.Writer;
import org.apache.distributedlog.exceptions.LogRecordTooLongException;
import org.apache.distributedlog.io.CompressionCodec.Type;
import org.junit.Test;

/**
 * Test Case for {@link LogRecordSet}.
 */
@Slf4j
public class TestLogRecordSet {

    @Test(timeout = 60000)
    public void testEmptyRecordSet() throws Exception {
        Writer writer = LogRecordSet.newWriter(1024, Type.NONE);
        assertEquals("zero user bytes", HEADER_LEN, writer.getNumBytes());
        assertEquals("zero records", 0, writer.getNumRecords());

        ByteBuf buffer = writer.getBuffer();
        assertEquals("zero user bytes", HEADER_LEN, buffer.readableBytes());

        LogRecordWithDLSN record = new LogRecordWithDLSN(
                new DLSN(1L, 0L, 0L),
                1L,
                buffer,
                1L);
        record.setRecordSet();
        Reader reader = LogRecordSet.of(record);
        assertNull("Empty record set should return null",
                reader.nextRecord());
        reader.release();
    }

    @Test(timeout = 60000)
    public void testWriteTooLongRecord() throws Exception {
        Writer writer = LogRecordSet.newWriter(1024, Type.NONE);
        assertEquals("zero user bytes", HEADER_LEN, writer.getNumBytes());
        assertEquals("zero records", 0, writer.getNumRecords());

        ByteBuffer dataBuf = ByteBuffer.allocate(MAX_LOGRECORD_SIZE + 1);
        try {
            writer.writeRecord(dataBuf, new CompletableFuture<DLSN>());
            fail("Should fail on writing large record");
        } catch (LogRecordTooLongException lrtle) {
            // expected
        }
        assertEquals("zero user bytes", HEADER_LEN, writer.getNumBytes());
        assertEquals("zero records", 0, writer.getNumRecords());
        ByteBuf buffer = writer.getBuffer();
        assertEquals("zero user bytes", HEADER_LEN, buffer.readableBytes());

        LogRecordWithDLSN record = new LogRecordWithDLSN(
                new DLSN(1L, 0L, 0L),
                1L,
                buffer,
                1L);
        record.setRecordSet();
        Reader reader = LogRecordSet.of(record);
        assertNull("Empty record set should return null",
                reader.nextRecord());
        reader.release();
    }

    @Test(timeout = 20000)
    public void testWriteRecordsNoneCompressed() throws Exception {
        testWriteRecords(Type.NONE);
    }

    @Test(timeout = 20000)
    public void testWriteRecordsLZ4Compressed() throws Exception {
        testWriteRecords(Type.LZ4);
    }

    void testWriteRecords(Type codec) throws Exception {
        Writer writer = LogRecordSet.newWriter(1024, codec);
        assertEquals("zero user bytes", HEADER_LEN, writer.getNumBytes());
        assertEquals("zero records", 0, writer.getNumRecords());

        List<CompletableFuture<DLSN>> writePromiseList = Lists.newArrayList();
        /// write first 5 records
        for (int i = 0; i < 5; i++) {
            ByteBuffer record = ByteBuffer.wrap(("record-" + i).getBytes(UTF_8));
            CompletableFuture<DLSN> writePromise = new CompletableFuture<>();
            writer.writeRecord(record, writePromise);
            writePromiseList.add(writePromise);
            assertEquals((i + 1) + " records", (i + 1), writer.getNumRecords());
        }
        ByteBuffer dataBuf = ByteBuffer.allocate(MAX_LOGRECORD_SIZE + 1);
        try {
            writer.writeRecord(dataBuf, new CompletableFuture<>());
            fail("Should fail on writing large record");
        } catch (LogRecordTooLongException lrtle) {
            // expected
        }
        assertEquals("5 records", 5, writer.getNumRecords());

        /// write another 5 records
        for (int i = 0; i < 5; i++) {
            ByteBuffer record = ByteBuffer.wrap(("record-" + (i + 5)).getBytes(UTF_8));
            CompletableFuture<DLSN> writePromise = new CompletableFuture<>();
            writer.writeRecord(record, writePromise);
            writePromiseList.add(writePromise);
            assertEquals((i + 6) + " records", (i + 6), writer.getNumRecords());
        }

        ByteBuf buffer = writer.getBuffer();
        assertEquals("10 records", 10, writer.getNumRecords());

        // Test transmit complete
        writer.completeTransmit(1L, 1L, 10L);
        List<DLSN> writeResults = FutureUtils.result(FutureUtils.collect(writePromiseList));
        for (int i = 0; i < 10; i++) {
            assertEquals(new DLSN(1L, 1L, 10L + i), writeResults.get(i));
        }

        LogRecordWithDLSN record = new LogRecordWithDLSN(
                new DLSN(1L, 1L, 10L),
                99L,
                buffer,
                999L);
        record.setPositionWithinLogSegment(888);
        record.setRecordSet();
        Reader reader = LogRecordSet.of(record);
        LogRecordWithDLSN readRecord = reader.nextRecord();
        int numReads = 0;
        while (null != readRecord) {
            assertEquals(new DLSN(1L, 1L, 10L + numReads), readRecord.getDlsn());
            assertEquals(99L, readRecord.getTransactionId());
            assertEquals(888 + numReads, readRecord.getPositionWithinLogSegment());
            assertEquals(999L, readRecord.getStartSequenceIdOfCurrentSegment());
            assertEquals(999L + 888 + numReads - 1, readRecord.getSequenceId());
            // read next
            ++numReads;
            readRecord = reader.nextRecord();
        }
        assertEquals(10, numReads);
        reader.release();
    }

}
