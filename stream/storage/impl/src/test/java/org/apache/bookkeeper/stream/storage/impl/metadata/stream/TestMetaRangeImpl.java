/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.stream.storage.impl.metadata.stream;

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.LongStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.RangeMetadata;
import org.apache.bookkeeper.stream.proto.RangeState;
import org.apache.bookkeeper.stream.proto.StreamMetadata.LifecycleState;
import org.apache.bookkeeper.stream.proto.StreamMetadata.ServingState;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.storage.impl.sc.StorageContainerPlacementPolicyImpl;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCAsyncStoreTestBase;
import org.junit.Test;

/**
 * Unit test of {@link MetaRangeImpl}.
 */
@Slf4j
public class TestMetaRangeImpl extends MVCCAsyncStoreTestBase {

    private StreamProperties streamProps;
    private MetaRangeImpl metaRange;

    @Override
    protected void doSetup() {
        this.streamProps = StreamProperties.newBuilder()
            .setStorageContainerId(1234L)
            .setStreamConf(DEFAULT_STREAM_CONF)
            .setStreamName(name.getMethodName() + "_stream")
            .setStreamId(System.currentTimeMillis())
            .build();
        this.metaRange = new MetaRangeImpl(
            this.store,
            this.scheduler.chooseThread(),
            StorageContainerPlacementPolicyImpl.of(1024));
    }

    @Override
    protected void doTeardown() throws Exception {
    }

    private <T> void assertIllegalStateException(CompletableFuture<T> future) throws Exception {
        try {
            future.get();
            fail("Should fail on illegal state");
        } catch (ExecutionException ee) {
            // expected.
            assertTrue(ee.getCause() instanceof IllegalStateException);
        }
    }

    @Test
    public void testCreate() throws Exception {
        assertTrue(FutureUtils.result(this.metaRange.create(streamProps)));

        verifyStreamMetadata(metaRange, streamProps);
    }

    @Test
    public void testLoad() throws Exception {
        assertTrue(FutureUtils.result(this.metaRange.create(streamProps)));

        MetaRangeImpl newMetaRange = new MetaRangeImpl(
            store,
            scheduler.chooseThread(),
            StorageContainerPlacementPolicyImpl.of(1024));
        assertNotNull(FutureUtils.result(newMetaRange.load(streamProps.getStreamId())));
        verifyStreamMetadata(newMetaRange, streamProps);
    }

    private void verifyStreamMetadata(MetaRangeImpl metaRange,
                                      StreamProperties streamProps)
        throws Exception {
        // verify the stream properties
        assertEquals(
            LifecycleState.CREATED,
            metaRange.unsafeGetLifecycleState());
        assertEquals(
            name.getMethodName() + "_stream",
            this.metaRange.getName());
        long cTime = metaRange.unsafeGetCreationTime();
        assertTrue(metaRange.unsafeGetCreationTime() == this.metaRange.unsafeGetModificationTime());
        assertEquals(streamProps, metaRange.unsafeGetStreamProperties());
        assertEquals(streamProps.getStreamId(), metaRange.unsafeGetStreamId());
        assertEquals(ServingState.WRITABLE, FutureUtils.result(metaRange.getServingState()));
        assertEquals(
            streamProps.getStreamConf(),
            FutureUtils.result(metaRange.getConfiguration()));

        // verify the stream ranges
        List<Long> activeRanges = Lists.transform(
            FutureUtils.result(metaRange.getActiveRanges()),
            (metadata) -> metadata.getProps().getRangeId()
        );
        assertEquals(streamProps.getStreamConf().getInitialNumRanges(), activeRanges.size());
        assertEquals(
            Lists.newArrayList(LongStream.range(1024L, 1024L + activeRanges.size()).iterator()),
            activeRanges);
        NavigableMap<Long, RangeMetadata> ranges = metaRange.unsafeGetRanges();
        long startKey = Long.MIN_VALUE;
        long rangeSize = Long.MAX_VALUE / (activeRanges.size() / 2);
        for (int idx = 0; idx < activeRanges.size(); ++idx) {
            long rid = activeRanges.get(idx);
            RangeMetadata rangeMetadata = ranges.get(rid);
            long endKey = startKey + rangeSize;
            if (idx == activeRanges.size() - 1) {
                endKey = Long.MAX_VALUE;
            }

            verifyRangeMetadata(rangeMetadata,
                startKey,
                endKey,
                rid,
                cTime,
                Long.MAX_VALUE,
                RangeState.RANGE_ACTIVE);

            readRangeMetadataAndVerify(streamProps.getStreamId(), rid,
                startKey,
                endKey,
                rid,
                cTime,
                Long.MAX_VALUE,
                RangeState.RANGE_ACTIVE);

            startKey = endKey;
        }
    }

    private void readRangeMetadataAndVerify(long streamId,
                                            long rangeId,
                                            long expectedStartKey,
                                            long expectedEndKey,
                                            long expectedRid,
                                            long expectedCTime,
                                            long expectedFenceTime,
                                            RangeState expectedRangeState) throws Exception {
        byte[] rangeKey = MetaRangeImpl.getStreamRangeKey(streamId, rangeId);
        byte[] rangeMetadataBytes = FutureUtils.result(store.get(rangeKey));
        RangeMetadata rangeMetadata = RangeMetadata.parseFrom(rangeMetadataBytes);

        verifyRangeMetadata(
            rangeMetadata,
            expectedStartKey,
            expectedEndKey,
            expectedRid,
            expectedCTime,
            expectedFenceTime,
            expectedRangeState);
    }

    private void verifyRangeMetadata(RangeMetadata metadata,
                                     long expectedStartKey,
                                     long expectedEndKey,
                                     long expectedRid,
                                     long expectedCTime,
                                     long expectedFenceTime,
                                     RangeState expectedRangeState) {
        assertEquals(expectedStartKey, metadata.getProps().getStartHashKey());
        assertEquals(expectedEndKey, metadata.getProps().getEndHashKey());
        assertEquals(expectedRid, metadata.getProps().getRangeId());
        assertEquals(expectedCTime, metadata.getCreateTime());
        assertEquals(expectedFenceTime, metadata.getFenceTime());
        assertEquals(expectedRangeState, metadata.getState());
    }

    @Test
    public void testDoubleCreate() throws Exception {
        // create first time
        assertTrue(FutureUtils.result(this.metaRange.create(streamProps)));
        // created again
        assertIllegalStateException(this.metaRange.create(streamProps));
    }

    @Test
    public void testUpdateServingState() throws Exception {
        // create first time
        assertTrue(FutureUtils.result(this.metaRange.create(streamProps)));
        long mTime = this.metaRange.unsafeGetModificationTime();
        assertEquals(
            ServingState.WRITABLE,
            FutureUtils.result(this.metaRange.getServingState()));
        long newTime = this.metaRange.unsafeGetModificationTime();
        assertTrue(newTime >= mTime);
        mTime = newTime;
        assertEquals(
            ServingState.READONLY,
            FutureUtils.result(this.metaRange.updateServingState(ServingState.READONLY)));
        newTime = this.metaRange.unsafeGetModificationTime();
        assertTrue(newTime >= mTime);
    }

    @Test
    public void testUpdateServingStateConcurrently() throws Exception {
        assertTrue(FutureUtils.result(this.metaRange.create(streamProps)));

        long mTime = this.metaRange.unsafeGetModificationTime();

        CompletableFuture<ServingState> updateFuture1 = metaRange.updateServingState(ServingState.WRITABLE);
        CompletableFuture<ServingState> updateFuture2 = metaRange.updateServingState(ServingState.READONLY);

        try {
            updateFuture1.get();
        } catch (Exception e) {
            assertEquals(ServingState.READONLY, updateFuture2.get());
            assertTrue(metaRange.unsafeGetModificationTime() >= mTime);
            assertEquals(ServingState.READONLY, FutureUtils.result(this.metaRange.getServingState()));
        }
        try {
            updateFuture2.get();
        } catch (Exception e) {
            assertEquals(ServingState.WRITABLE, updateFuture1.get());
            assertTrue(metaRange.unsafeGetModificationTime() >= mTime);
            assertEquals(ServingState.WRITABLE, FutureUtils.result(this.metaRange.getServingState()));
        }
    }

}
