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
package org.apache.distributedlog.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.distributedlog.util.DLUtils.validateAndNormalizeName;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.distributedlog.DLMTestUtil;
import org.apache.distributedlog.LogSegmentMetadata;
import org.apache.distributedlog.LogSegmentMetadata.LogSegmentMetadataVersion;
import org.apache.distributedlog.exceptions.InvalidStreamNameException;
import org.apache.distributedlog.exceptions.UnexpectedException;
import org.junit.Test;




/**
 * Test Case for {@link DLUtils}.
 */
public class TestDLUtils {

    private static LogSegmentMetadata completedLogSegment(
            long logSegmentSequenceNumber,
            long fromTxnId,
            long toTxnId) {
        return completedLogSegment(
                logSegmentSequenceNumber,
                fromTxnId,
                toTxnId,
                LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
    }

    private static LogSegmentMetadata completedLogSegment(
        long logSegmentSequenceNumber,
        long fromTxnId,
        long toTxnId,
        int version) {
        return DLMTestUtil.completedLogSegment(
                "/logsegment/" + fromTxnId,
                fromTxnId,
                fromTxnId,
                toTxnId,
                100,
                logSegmentSequenceNumber,
                999L,
                0L,
                version);
    }

    private static LogSegmentMetadata inprogressLogSegment(
            long logSegmentSequenceNumber, long firstTxId) {
        return inprogressLogSegment(
                logSegmentSequenceNumber,
                firstTxId,
                LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
    }

    private static LogSegmentMetadata inprogressLogSegment(
            long logSegmentSequenceNumber, long firstTxId, int version) {
        return DLMTestUtil.inprogressLogSegment(
                "/logsegment/" + firstTxId,
                firstTxId,
                firstTxId,
                logSegmentSequenceNumber,
                version);
    }

    @Test(timeout = 60000)
    public void testFindLogSegmentNotLessThanTxnId() throws Exception {
        long txnId = 999L;
        // empty list
        List<LogSegmentMetadata> emptyList = Lists.newArrayList();
        assertEquals(-1, DLUtils.findLogSegmentNotLessThanTxnId(emptyList, txnId));

        // list that all segment's txn id is larger than txn-id-to-search
        List<LogSegmentMetadata> list1 = Lists.newArrayList(
                completedLogSegment(1L, 1000L, 2000L));
        assertEquals(-1, DLUtils.findLogSegmentNotLessThanTxnId(list1, txnId));

        List<LogSegmentMetadata> list2 = Lists.newArrayList(
                inprogressLogSegment(1L, 1000L));
        assertEquals(-1, DLUtils.findLogSegmentNotLessThanTxnId(list2, txnId));

        // the first log segment whose first txn id is less than txn-id-to-search
        List<LogSegmentMetadata> list3 = Lists.newArrayList(
                completedLogSegment(1L, 0L, 99L),
                completedLogSegment(2L, 1000L, 2000L)
        );
        assertEquals(1, DLUtils.findLogSegmentNotLessThanTxnId(list3, txnId));

        List<LogSegmentMetadata> list4 = Lists.newArrayList(
                completedLogSegment(1L, 0L, 990L),
                completedLogSegment(2L, 1000L, 2000L)
        );
        assertEquals(1, DLUtils.findLogSegmentNotLessThanTxnId(list4, txnId));

        List<LogSegmentMetadata> list5 = Lists.newArrayList(
                inprogressLogSegment(1L, 0L),
                inprogressLogSegment(2L, 1000L)
        );
        assertEquals(0, DLUtils.findLogSegmentNotLessThanTxnId(list5, txnId));

        // list that all segment's txn id is less than txn-id-to-search
        List<LogSegmentMetadata> list6_0 = Lists.newArrayList(
                completedLogSegment(1L, 100L, 200L));
        assertEquals(0, DLUtils.findLogSegmentNotLessThanTxnId(list6_0, txnId));

        List<LogSegmentMetadata> list6_1 = Lists.newArrayList(
                completedLogSegment(1L, 100L, 199L),
                completedLogSegment(2L, 200L, 299L));
        assertEquals(1, DLUtils.findLogSegmentNotLessThanTxnId(list6_1, txnId));

        List<LogSegmentMetadata> list7 = Lists.newArrayList(
                inprogressLogSegment(1L, 100L));
        assertEquals(0, DLUtils.findLogSegmentNotLessThanTxnId(list7, txnId));

        // list that first segment's first txn id equals to txn-id-to-search
        List<LogSegmentMetadata> list8 = Lists.newArrayList(
                completedLogSegment(1L, 999L, 2000L));
        assertEquals(0, DLUtils.findLogSegmentNotLessThanTxnId(list8, txnId));

        List<LogSegmentMetadata> list9 = Lists.newArrayList(
                inprogressLogSegment(1L, 999L));
        assertEquals(0, DLUtils.findLogSegmentNotLessThanTxnId(list9, txnId));

        List<LogSegmentMetadata> list10 = Lists.newArrayList(
                completedLogSegment(1L, 0L, 999L),
                completedLogSegment(2L, 999L, 2000L));
        assertEquals(0, DLUtils.findLogSegmentNotLessThanTxnId(list10, txnId));

        List<LogSegmentMetadata> list11 = Lists.newArrayList(
                completedLogSegment(1L, 0L, 99L),
                completedLogSegment(2L, 999L, 2000L));
        assertEquals(1, DLUtils.findLogSegmentNotLessThanTxnId(list11, txnId));

        List<LogSegmentMetadata> list12 = Lists.newArrayList(
                inprogressLogSegment(1L, 0L),
                inprogressLogSegment(2L, 999L));
        assertEquals(1, DLUtils.findLogSegmentNotLessThanTxnId(list12, txnId));
    }

    @Test(timeout = 60000)
    public void testNextLogSegmentSequenceNumber() throws Exception {
        List<LogSegmentMetadata> v1List = Lists.newArrayList(
                completedLogSegment(2L, 100L, 199L, LogSegmentMetadataVersion.VERSION_V1_ORIGINAL.value),
                completedLogSegment(1L, 0L, 99L, LogSegmentMetadataVersion.VERSION_V1_ORIGINAL.value));
        assertNull(DLUtils.nextLogSegmentSequenceNumber(v1List));

        List<LogSegmentMetadata> afterV1List = Lists.newArrayList(
                completedLogSegment(2L, 100L, 199L),
                completedLogSegment(1L, 0L, 99L));
        assertEquals((Long) 3L, DLUtils.nextLogSegmentSequenceNumber(afterV1List));

        List<LogSegmentMetadata> mixList1 = Lists.newArrayList(
                completedLogSegment(2L, 100L, 199L, LogSegmentMetadataVersion.VERSION_V1_ORIGINAL.value),
                completedLogSegment(1L, 0L, 99L));
        assertEquals((Long) 3L, DLUtils.nextLogSegmentSequenceNumber(mixList1));

        List<LogSegmentMetadata> mixList2 = Lists.newArrayList(
                completedLogSegment(2L, 100L, 199L),
                completedLogSegment(1L, 0L, 99L, LogSegmentMetadataVersion.VERSION_V1_ORIGINAL.value));
        assertEquals((Long) 3L, DLUtils.nextLogSegmentSequenceNumber(mixList2));
    }

    @Test(timeout = 60000, expected = UnexpectedException.class)
    public void testUnexpectedExceptionOnComputeStartSequenceId() throws Exception {
        List<LogSegmentMetadata> segments = Lists.newArrayList(
                inprogressLogSegment(3L, 201L),
                inprogressLogSegment(2L, 101L),
                completedLogSegment(1L, 1L, 100L).mutator().setStartSequenceId(1L).build()
        );
        DLUtils.computeStartSequenceId(segments, segments.get(0));
    }

    @Test(timeout = 60000)
    public void testComputeStartSequenceIdOnEmptyList() throws Exception {
        List<LogSegmentMetadata> emptyList = Lists.newArrayList();
        assertEquals(0L, DLUtils.computeStartSequenceId(emptyList, inprogressLogSegment(1L, 1L)));
    }

    @Test(timeout = 60000)
    public void testComputeStartSequenceIdOnLowerSequenceNumberSegment() throws Exception {
        List<LogSegmentMetadata> segments = Lists.newArrayList(
                completedLogSegment(3L, 201L, 300L).mutator().setStartSequenceId(201L).build(),
                completedLogSegment(2L, 101L, 200L).mutator().setStartSequenceId(101L).build()
        );
        assertEquals(0L, DLUtils.computeStartSequenceId(segments, inprogressLogSegment(1L, 1L)));
    }

    @Test(timeout = 60000)
    public void testComputeStartSequenceIdOnHigherSequenceNumberSegment() throws Exception {
        List<LogSegmentMetadata> segments = Lists.newArrayList(
                completedLogSegment(3L, 201L, 300L).mutator().setStartSequenceId(201L).build(),
                completedLogSegment(2L, 101L, 200L).mutator().setStartSequenceId(101L).build()
        );
        assertEquals(0L, DLUtils.computeStartSequenceId(segments, inprogressLogSegment(5L, 401L)));
    }

    @Test(timeout = 60000)
    public void testComputeStartSequenceId() throws Exception {
        List<LogSegmentMetadata> segments = Lists.newArrayList(
                completedLogSegment(3L, 201L, 300L).mutator()
                        .setStartSequenceId(201L).setRecordCount(100).build(),
                completedLogSegment(2L, 101L, 200L).mutator()
                        .setStartSequenceId(101L).setRecordCount(100).build()
        );
        assertEquals(301L, DLUtils.computeStartSequenceId(segments, inprogressLogSegment(4L, 301L)));
    }

    @Test(timeout = 60000)
    public void testSerDeLogSegmentSequenceNumber() throws Exception {
        long sn = 123456L;
        byte[] snData = Long.toString(sn).getBytes(UTF_8);
        assertEquals("Deserialization should succeed",
                sn, DLUtils.deserializeLogSegmentSequenceNumber(snData));
        assertArrayEquals("Serialization should succeed",
                snData, DLUtils.serializeLogSegmentSequenceNumber(sn));
    }

    @Test(timeout = 60000, expected = NumberFormatException.class)
    public void testDeserilizeInvalidLSSN() throws Exception {
        byte[] corruptedData = "corrupted-lssn".getBytes(UTF_8);
        DLUtils.deserializeLogSegmentSequenceNumber(corruptedData);
    }

    @Test(timeout = 60000)
    public void testSerDeLogRecordTxnId() throws Exception {
        long txnId = 123456L;
        byte[] txnData = Long.toString(txnId).getBytes(UTF_8);
        assertEquals("Deserialization should succeed",
                txnId, DLUtils.deserializeTransactionId(txnData));
        assertArrayEquals("Serialization should succeed",
                txnData, DLUtils.serializeTransactionId(txnId));
    }

    @Test(timeout = 60000, expected = NumberFormatException.class)
    public void testDeserilizeInvalidLogRecordTxnId() throws Exception {
        byte[] corruptedData = "corrupted-txn-id".getBytes(UTF_8);
        DLUtils.deserializeTransactionId(corruptedData);
    }

    @Test(timeout = 60000)
    public void testSerDeLedgerId() throws Exception {
        long ledgerId = 123456L;
        byte[] ledgerIdData = Long.toString(ledgerId).getBytes(UTF_8);
        assertEquals("Deserialization should succeed",
                ledgerId, DLUtils.bytes2LogSegmentId(ledgerIdData));
        assertArrayEquals("Serialization should succeed",
                ledgerIdData, DLUtils.logSegmentId2Bytes(ledgerId));
    }

    @Test(timeout = 60000, expected = NumberFormatException.class)
    public void testDeserializeInvalidLedgerId() throws Exception {
        byte[] corruptedData = "corrupted-ledger-id".getBytes(UTF_8);
        DLUtils.bytes2LogSegmentId(corruptedData);
    }

    @Test(timeout = 10000)
    public void testValidateLogName() throws Exception {
        String logName = "test-validate-log-name";
        validateAndNormalizeName(logName);
    }

    @Test(timeout = 10000, expected = InvalidStreamNameException.class)
    public void testValidateBadLogName0() throws Exception {
        String logName = "  test-bad-log-name";
        validateAndNormalizeName(logName);
    }

    @Test(timeout = 10000, expected = InvalidStreamNameException.class)
    public void testValidateBadLogName1() throws Exception {
        String logName = "test-bad-log-name/";
        validateAndNormalizeName(logName);
    }

    @Test(timeout = 10000, expected = InvalidStreamNameException.class)
    public void testValidateBadLogName2() throws Exception {
        String logName = "../test-bad-log-name/";
        validateAndNormalizeName(logName);
    }

    @Test(timeout = 10000)
    public void testValidateSameStreamPath() throws Exception {
        String logName1 = "/test-resolve-log";
        String logName2 = "test-resolve-log";
        validateAndNormalizeName(logName1);
        validateAndNormalizeName(logName2);
    }

}
