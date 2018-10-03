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
package org.apache.bookkeeper.stream.storage.impl.metadata;

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.stream.LongStream;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.RangeMetadata;
import org.apache.bookkeeper.stream.proto.RangeState;
import org.apache.bookkeeper.stream.proto.StreamMetadata.LifecycleState;
import org.apache.bookkeeper.stream.proto.StreamMetadata.ServingState;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesRequest;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesResponse;
import org.apache.bookkeeper.stream.proto.storage.RelatedRanges;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.storage.impl.metadata.stream.MetaRangeImpl;
import org.apache.bookkeeper.stream.storage.impl.sc.StorageContainerPlacementPolicyImpl;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCAsyncStoreTestBase;
import org.junit.Test;

/**
 * Unit test of {@link MetaRangeStoreImplTest}.
 */
public class MetaRangeStoreImplTest extends MVCCAsyncStoreTestBase {

    private StreamProperties streamProps;
    private MetaRangeStoreImpl mrStoreImpl;
    private StorageServerClientManager clientManager;

    @Override
    protected void doSetup() throws Exception {
        this.streamProps = StreamProperties.newBuilder()
            .setStorageContainerId(1234L)
            .setStreamConf(DEFAULT_STREAM_CONF)
            .setStreamName(name.getMethodName() + "_stream")
            .setStreamId(System.currentTimeMillis())
            .build();
        this.clientManager = mock(StorageServerClientManager.class);
        this.mrStoreImpl = new MetaRangeStoreImpl(
            this.store,
            StorageContainerPlacementPolicyImpl.of(1024),
            this.scheduler.chooseThread(),
            clientManager);
    }

    @Override
    protected void doTeardown() throws Exception {
    }

    GetActiveRangesRequest createRequest(StreamProperties streamProperties) {
        when(clientManager.getStreamProperties(eq(this.streamProps.getStreamId())))
            .thenReturn(FutureUtils.value(streamProperties));
        GetActiveRangesRequest.Builder reqBuilder = GetActiveRangesRequest.newBuilder()
            .setStreamId(this.streamProps.getStreamId());
        return reqBuilder.build();
    }

    @Test
    public void testCreateIfMissingPropsNotSpecified() throws Exception {
        GetActiveRangesResponse resp = FutureUtils.result(
            this.mrStoreImpl.getActiveRanges(createRequest(null)));

        assertEquals(StatusCode.STREAM_NOT_FOUND, resp.getCode());
    }

    @Test
    public void testCreateIfMissing() throws Exception {
        GetActiveRangesResponse resp = FutureUtils.result(
            this.mrStoreImpl.getActiveRanges(createRequest(streamProps)));

        assertEquals(StatusCode.SUCCESS, resp.getCode());
        verifyGetResponse(resp);
    }

    private void verifyGetResponse(GetActiveRangesResponse getResp) throws Exception {
        MetaRangeImpl metaRange = new MetaRangeImpl(
            this.store,
            this.scheduler.chooseThread(),
            StorageContainerPlacementPolicyImpl.of(1024));
        assertNotNull(FutureUtils.result(metaRange.load(streamProps.getStreamId())));
        verifyStreamMetadata(metaRange, streamProps);

        List<RelatedRanges> rangesList = getResp.getRangesList();
        List<Long> currentRanges = metaRange.unsafeGetCurrentRanges();

        assertEquals(currentRanges.size(), rangesList.size());
        for (int i = 0; i < rangesList.size(); i++) {
            RelatedRanges actualRR = rangesList.get(i);
            long expectedRid = currentRanges.get(i);
            RangeMetadata expectedRangeMetadata =
                metaRange.unsafeGetRanges().get(expectedRid);
            assertNotNull(expectedRangeMetadata);

            assertEquals(Collections.emptyList(), actualRR.getRelatedRangesList());
            assertEquals(expectedRangeMetadata.getProps(), actualRR.getProps());
        }

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
            metaRange.getName());
        long cTime = metaRange.unsafeGetCreationTime();
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
    public void testGetTwice() throws Exception {
        GetActiveRangesResponse resp = FutureUtils.result(
            this.mrStoreImpl.getActiveRanges(createRequest(streamProps)));

        assertEquals(StatusCode.SUCCESS, resp.getCode());

        verifyGetResponse(resp);

        GetActiveRangesResponse secondResp = FutureUtils.result(
            this.mrStoreImpl.getActiveRanges(createRequest(streamProps)));

        assertEquals(StatusCode.SUCCESS, secondResp.getCode());

        verifyGetResponse(secondResp);
    }

}
