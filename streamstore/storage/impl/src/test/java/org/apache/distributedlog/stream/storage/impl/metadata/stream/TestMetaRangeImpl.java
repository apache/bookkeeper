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

package org.apache.distributedlog.stream.storage.impl.metadata.stream;

import static org.apache.distributedlog.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.LongStream;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.stream.proto.RangeMetadata;
import org.apache.distributedlog.stream.proto.RangeState;
import org.apache.distributedlog.stream.proto.StreamMetadata;
import org.apache.distributedlog.stream.proto.StreamMetadata.LifecycleState;
import org.apache.distributedlog.stream.proto.StreamMetadata.ServingState;
import org.apache.distributedlog.stream.proto.StreamProperties;
import org.apache.distributedlog.stream.storage.impl.sc.StorageContainerPlacementPolicyImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

/**
 * Unit test of {@link MetaRangeImpl}.
 */
public class TestMetaRangeImpl {

  @Rule
  public final TestName runtime = new TestName();
  @Rule
  public final Timeout globalTimeout = Timeout.seconds(10);

  private StreamProperties streamProps;
  private MetaRangeImpl metaRange;
  private ScheduledExecutorService executor;

  @Before
  public void setUp() {
    this.streamProps = StreamProperties.newBuilder()
      .setStorageContainerId(1234L)
      .setStreamConf(DEFAULT_STREAM_CONF)
      .setStreamName(runtime.getMethodName() + "_stream")
      .setStreamId(System.currentTimeMillis())
      .build();
    this.executor = Executors.newSingleThreadScheduledExecutor();
    this.metaRange = new MetaRangeImpl(
      this.executor,
      StorageContainerPlacementPolicyImpl.of(1024));
  }

  @After
  public void tearDown() {
    if (null != this.executor) {
      this.executor.shutdown();
    }
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
  @Ignore
  public void testOperationsBeforeCreated() throws Exception {
    try {
      this.metaRange.getName();
      fail("Should fail on illegal state");
    } catch (IllegalStateException ise) {
      // expected.
      assertTrue(true);
    }

    assertIllegalStateException(metaRange.getServingState());
    assertIllegalStateException(metaRange.updateServingState(StreamMetadata.ServingState.READONLY));
    assertIllegalStateException(metaRange.getConfiguration());
    assertIllegalStateException(metaRange.getActiveRanges());
  }

  @Test
  public void testCreate() throws Exception {
    assertTrue(FutureUtils.result(this.metaRange.create(streamProps)));

    // verify the stream properties
    assertEquals(
      LifecycleState.CREATED,
      this.metaRange.unsafeGetLifecycleState());
    assertEquals(
      runtime.getMethodName() + "_stream",
      this.metaRange.getName());
    long cTime = this.metaRange.unsafeGetCreationTime();
    assertTrue(this.metaRange.unsafeGetCreationTime() == this.metaRange.unsafeGetModificationTime());
    assertEquals(streamProps, this.metaRange.unsafeGetStreamProperties());
    assertEquals(streamProps.getStreamId(), this.metaRange.unsafeGetStreamId());
    assertEquals(ServingState.WRITABLE, FutureUtils.result(this.metaRange.getServingState()));
    assertEquals(
      streamProps.getStreamConf(),
      FutureUtils.result(this.metaRange.getConfiguration()));

    // verify the stream ranges
    List<Long> activeRanges = Lists.transform(
      FutureUtils.result(this.metaRange.getActiveRanges()),
      (metadata) -> metadata.getProps().getRangeId()
    );
    assertEquals(streamProps.getStreamConf().getInitialNumRanges(), activeRanges.size());
    assertEquals(
      Lists.newArrayList(LongStream.range(1024L, 1024L + activeRanges.size()).iterator()),
      activeRanges);
    NavigableMap<Long, RangeMetadata> ranges = this.metaRange.unsafeGetRanges();
    long startKey = Long.MIN_VALUE;
    long rangeSize = Long.MAX_VALUE / (activeRanges.size() / 2);
    for (int idx = 0; idx < activeRanges.size(); ++idx) {
      long rid = activeRanges.get(idx);
      RangeMetadata rangeMetadata = ranges.get(rid);
      long endKey = startKey + rangeSize;
      if (idx == activeRanges.size() - 1) {
        endKey = Long.MAX_VALUE;
      }
      assertEquals(startKey, rangeMetadata.getProps().getStartHashKey());
      assertEquals(endKey, rangeMetadata.getProps().getEndHashKey());
      assertEquals(rid, rangeMetadata.getProps().getRangeId());
      assertEquals(cTime, rangeMetadata.getCreateTime());
      assertEquals(Long.MAX_VALUE, rangeMetadata.getFenceTime());
      assertEquals(RangeState.RANGE_ACTIVE, rangeMetadata.getState());

      startKey = endKey;
    }
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

}
