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
package org.apache.distributedlog.logsegment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.distributedlog.DLMTestUtil;
import org.apache.distributedlog.LogSegmentMetadata;
import org.apache.distributedlog.exceptions.UnexpectedException;
import org.junit.Test;



/**
 * Test Case for Per Stream Log Segment Cache.
 */
public class TestPerStreamLogSegmentCache {

    @Test(timeout = 60000)
    public void testBasicOperations() {
        LogSegmentMetadata metadata =
                DLMTestUtil.completedLogSegment("/segment1", 1L, 1L, 100L, 100, 1L, 99L, 0L);
        String name = DLMTestUtil.completedLedgerZNodeNameWithLogSegmentSequenceNumber(1L);

        PerStreamLogSegmentCache cache = new PerStreamLogSegmentCache("test-basic-operations");
        assertNull("No log segment " + name + " should be cached", cache.get(name));
        cache.add(name, metadata);
        LogSegmentMetadata metadataRetrieved = cache.get(name);
        assertNotNull("log segment " + name + " should be cached", metadataRetrieved);
        assertEquals("Wrong log segment metadata returned for " + name,
                metadata, metadataRetrieved);
        LogSegmentMetadata metadataRemoved = cache.remove(name);
        assertNull("log segment " + name + " should be removed from cache", cache.get(name));
        assertEquals("Wrong log segment metadata removed for " + name,
                metadata, metadataRemoved);
        assertNull("No log segment " + name + " to be removed", cache.remove(name));
    }

    @Test(timeout = 60000)
    public void testDiff() {
        PerStreamLogSegmentCache cache = new PerStreamLogSegmentCache("test-diff");
        // add 5 completed log segments
        for (int i = 1; i <= 5; i++) {
            LogSegmentMetadata metadata =
                    DLMTestUtil.completedLogSegment("/segment" + i, i, i, i * 100L, 100, i, 99L, 0L);
            String name = DLMTestUtil.completedLedgerZNodeNameWithLogSegmentSequenceNumber(i);
            cache.add(name, metadata);
        }
        // add one inprogress log segment
        LogSegmentMetadata inprogress =
                DLMTestUtil.inprogressLogSegment("/inprogress-6", 6, 600L, 6);
        String name = DLMTestUtil.inprogressZNodeName(6);
        cache.add(name, inprogress);

        // deleted first 2 completed log segments and completed the last one
        Set<String> segmentRemoved = Sets.newHashSet();
        for (int i = 1; i <= 2; i++) {
            segmentRemoved.add(DLMTestUtil.completedLedgerZNodeNameWithLogSegmentSequenceNumber(i));
        }
        segmentRemoved.add((DLMTestUtil.inprogressZNodeName(6)));
        Set<String> segmentReceived = Sets.newHashSet();
        Set<String> segmentAdded = Sets.newHashSet();
        for (int i = 3; i <= 6; i++) {
            segmentReceived.add(DLMTestUtil.completedLedgerZNodeNameWithLogSegmentSequenceNumber(i));
            if (i == 6) {
                segmentAdded.add(DLMTestUtil.completedLedgerZNodeNameWithLogSegmentSequenceNumber(i));
            }
        }

        Pair<Set<String>, Set<String>> segmentChanges = cache.diff(segmentReceived);
        assertTrue("Should remove " + segmentRemoved + ", but removed " + segmentChanges.getRight(),
                Sets.difference(segmentRemoved, segmentChanges.getRight()).isEmpty());
        assertTrue("Should add " + segmentAdded + ", but added " + segmentChanges.getLeft(),
                Sets.difference(segmentAdded, segmentChanges.getLeft()).isEmpty());
    }

    @Test(timeout = 60000)
    public void testUpdate() {
        PerStreamLogSegmentCache cache = new PerStreamLogSegmentCache("test-update");
        // add 5 completed log segments
        for (int i = 1; i <= 5; i++) {
            LogSegmentMetadata metadata =
                    DLMTestUtil.completedLogSegment("/segment" + i, i, i, i * 100L, 100, i, 99L, 0L);
            String name = DLMTestUtil.completedLedgerZNodeNameWithLogSegmentSequenceNumber(i);
            cache.add(name, metadata);
        }
        // add one inprogress log segment
        LogSegmentMetadata inprogress =
                DLMTestUtil.inprogressLogSegment("/inprogress-6", 6, 600L, 6);
        String name = DLMTestUtil.inprogressZNodeName(6);
        cache.add(name, inprogress);

        // deleted first 2 completed log segments and completed the last one
        Set<String> segmentRemoved = Sets.newHashSet();
        for (int i = 1; i <= 2; i++) {
            segmentRemoved.add(DLMTestUtil.completedLedgerZNodeNameWithLogSegmentSequenceNumber(i));
        }
        segmentRemoved.add((DLMTestUtil.inprogressZNodeName(6)));
        Set<String> segmentReceived = Sets.newHashSet();
        Map<String, LogSegmentMetadata> segmentAdded = Maps.newHashMap();
        for (int i = 3; i <= 6; i++) {
            segmentReceived.add(DLMTestUtil.completedLedgerZNodeNameWithLogSegmentSequenceNumber(i));
            if (i == 6) {
                segmentAdded.put(DLMTestUtil.completedLedgerZNodeNameWithLogSegmentSequenceNumber(i),
                        DLMTestUtil.completedLogSegment("/segment" + i, i, i, i * 100L, 100, i, 99L, 0L));
            }
        }

        // update the cache
        cache.update(segmentRemoved, segmentAdded);
        for (String segment : segmentRemoved) {
            assertNull("Segment " + segment + " should be removed.", cache.get(segment));
        }
        for (String segment : segmentReceived) {
            assertNotNull("Segment " + segment + " should not be removed", cache.get(segment));
        }
        for (Map.Entry<String, LogSegmentMetadata> entry : segmentAdded.entrySet()) {
            assertEquals("Segment " + entry.getKey() + " should be added.",
                    entry.getValue(), entry.getValue());
        }
    }

    @Test(timeout = 60000, expected = UnexpectedException.class)
    public void testGapDetection() throws Exception {
        PerStreamLogSegmentCache cache = new PerStreamLogSegmentCache("test-gap-detection");
        cache.add(DLMTestUtil.completedLedgerZNodeNameWithLogSegmentSequenceNumber(1L),
                DLMTestUtil.completedLogSegment("/segment-1", 1L, 1L, 100L, 100, 1L, 99L, 0L));
        cache.add(DLMTestUtil.completedLedgerZNodeNameWithLogSegmentSequenceNumber(3L),
                DLMTestUtil.completedLogSegment("/segment-3", 3L, 3L, 300L, 100, 3L, 99L, 0L));
        cache.getLogSegments(LogSegmentMetadata.COMPARATOR);
    }

    @Test(timeout = 60000)
    public void testGapDetectionOnLogSegmentsWithoutLogSegmentSequenceNumber() throws Exception {
        PerStreamLogSegmentCache cache = new PerStreamLogSegmentCache("test-gap-detection");
        LogSegmentMetadata segment1 =
                DLMTestUtil.completedLogSegment("/segment-1", 1L, 1L, 100L, 100, 1L, 99L, 0L)
                        .mutator().setVersion(LogSegmentMetadata.LogSegmentMetadataVersion.VERSION_V1_ORIGINAL).build();
        cache.add(DLMTestUtil.completedLedgerZNodeNameWithLogSegmentSequenceNumber(1L), segment1);
        LogSegmentMetadata segment3 =
                DLMTestUtil.completedLogSegment("/segment-3", 3L, 3L, 300L, 100, 3L, 99L, 0L)
                .mutator().setVersion(LogSegmentMetadata.LogSegmentMetadataVersion.VERSION_V2_LEDGER_SEQNO).build();
        cache.add(DLMTestUtil.completedLedgerZNodeNameWithLogSegmentSequenceNumber(3L), segment3);
        List<LogSegmentMetadata> expectedList = Lists.asList(segment1, new LogSegmentMetadata[] { segment3 });
        List<LogSegmentMetadata> resultList = cache.getLogSegments(LogSegmentMetadata.COMPARATOR);
        assertEquals(expectedList, resultList);
    }

    @Test(timeout = 60000, expected = UnexpectedException.class)
    public void testSameLogSegment() throws Exception {
        PerStreamLogSegmentCache cache = new PerStreamLogSegmentCache("test-same-log-segment");
        List<LogSegmentMetadata> expectedList = Lists.newArrayListWithExpectedSize(2);
        LogSegmentMetadata inprogress =
                DLMTestUtil.inprogressLogSegment("/inprogress-1", 1L, 1L, 1L);
        expectedList.add(inprogress);
        cache.add(DLMTestUtil.inprogressZNodeName(1L), inprogress);
        LogSegmentMetadata completed =
                DLMTestUtil.completedLogSegment("/segment-1", 1L, 1L, 100L, 100, 1L, 99L, 0L);
        expectedList.add(completed);
        cache.add(DLMTestUtil.completedLedgerZNodeNameWithLogSegmentSequenceNumber(1L), completed);

        cache.getLogSegments(LogSegmentMetadata.COMPARATOR);
    }

}
