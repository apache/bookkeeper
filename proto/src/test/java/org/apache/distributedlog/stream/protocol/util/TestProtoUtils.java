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

package org.apache.distributedlog.stream.protocol.util;

import static org.apache.distributedlog.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createCreateCollectionRequest;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createDeleteCollectionRequest;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createGetCollectionRequest;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.isStreamCreated;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.isStreamWritable;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.keyRangeOverlaps;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.validateCollectionName;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.validateStreamName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.distributedlog.stream.proto.CollectionConfiguration;
import org.apache.distributedlog.stream.proto.RangeProperties;
import org.apache.distributedlog.stream.proto.StreamMetadata.LifecycleState;
import org.apache.distributedlog.stream.proto.StreamMetadata.ServingState;
import org.apache.distributedlog.stream.proto.rangeservice.CreateCollectionRequest;
import org.apache.distributedlog.stream.proto.rangeservice.DeleteCollectionRequest;
import org.apache.distributedlog.stream.proto.rangeservice.GetCollectionRequest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test of {@link ProtoUtils}.
 */
public class TestProtoUtils {

  @Rule
  public final TestName name = new TestName();

  @Test
  public void testValidateCollectionName() {
    assertTrue(validateCollectionName("collection_name"));
    assertTrue(validateCollectionName("CollectionName"));
    assertTrue(validateCollectionName("9CollectionName"));
    assertFalse(validateCollectionName("collection-name"));
    assertFalse(validateCollectionName("!collection_name"));
  }

  @Test
  public void testValidateStreamName() {
    assertTrue(validateStreamName("stream_name"));
    assertTrue(validateStreamName("StreamName"));
    assertTrue(validateStreamName("9StreamName"));
    assertFalse(validateStreamName("stream-name"));
    assertFalse(validateStreamName("!stream_name"));
  }

  @Test
  public void testKeyRangeOverlaps1() {
    assertFalse(keyRangeOverlaps(1000L, 2000L, 3000L, 4000L));
    assertTrue(keyRangeOverlaps(1000L, 2000L, 1500L, 2500L));
    assertTrue(keyRangeOverlaps(1000L, 2000L, 1500L, 1800L));
    assertTrue(keyRangeOverlaps(1000L, 3500L, 3000L, 4000L));
    assertTrue(keyRangeOverlaps(3200L, 3500L, 3000L, 4000L));
  }

  @Test
  public void testKeyRangeOverlaps2() {
    assertFalse(keyRangeOverlaps(
      Pair.of(1000L, 2000L),
      Pair.of(3000L, 4000L)));
    assertTrue(keyRangeOverlaps(
      Pair.of(1000L, 2000L),
      Pair.of(1500L, 2500L)));
    assertTrue(keyRangeOverlaps(
      Pair.of(1000L, 2000L),
      Pair.of(1500L, 1800L)));
    assertTrue(keyRangeOverlaps(
      Pair.of(1000L, 3500L),
      Pair.of(3000L, 4000L)));
    assertTrue(keyRangeOverlaps(
      Pair.of(3200L, 3500L),
      Pair.of(3000L, 4000L)));
  }

  private static RangeProperties createRangeMeta(long startKey, long endKey) {
    return RangeProperties.newBuilder()
      .setStartHashKey(startKey)
      .setEndHashKey(endKey)
      .setStorageContainerId(1234L)
      .setRangeId(1234L)
      .build();
  }

  @Test
  public void testKeyRangeOverlaps3() {
    assertFalse(keyRangeOverlaps(
      createRangeMeta(1000L, 2000L),
      createRangeMeta(3000L, 4000L)));
    assertTrue(keyRangeOverlaps(
      createRangeMeta(1000L, 2000L),
      createRangeMeta(1500L, 2500L)));
    assertTrue(keyRangeOverlaps(
      createRangeMeta(1000L, 2000L),
      createRangeMeta(1500L, 1800L)));
    assertTrue(keyRangeOverlaps(
      createRangeMeta(1000L, 3500L),
      createRangeMeta(3000L, 4000L)));
    assertTrue(keyRangeOverlaps(
      createRangeMeta(3200L, 3500L),
      createRangeMeta(3000L, 4000L)));
  }

  @Test
  public void testKeyRangeOverlaps4() {
    assertFalse(keyRangeOverlaps(
      createRangeMeta(1000L, 2000L),
      Pair.of(3000L, 4000L)));
    assertTrue(keyRangeOverlaps(
      createRangeMeta(1000L, 2000L),
      Pair.of(1500L, 2500L)));
    assertTrue(keyRangeOverlaps(
      createRangeMeta(1000L, 2000L),
      Pair.of(1500L, 1800L)));
    assertTrue(keyRangeOverlaps(
      createRangeMeta(1000L, 3500L),
      Pair.of(3000L, 4000L)));
    assertTrue(keyRangeOverlaps(
      createRangeMeta(3200L, 3500L),
      Pair.of(3000L, 4000L)));
  }

  @Test
  public void testKeyRangeOverlaps5() {
    assertFalse(keyRangeOverlaps(
      Pair.of(1000L, 2000L),
      createRangeMeta(3000L, 4000L)));
    assertTrue(keyRangeOverlaps(
      Pair.of(1000L, 2000L),
      createRangeMeta(1500L, 2500L)));
    assertTrue(keyRangeOverlaps(
      Pair.of(1000L, 2000L),
      createRangeMeta(1500L, 1800L)));
    assertTrue(keyRangeOverlaps(
      Pair.of(1000L, 3500L),
      createRangeMeta(3000L, 4000L)));
    assertTrue(keyRangeOverlaps(
      Pair.of(3200L, 3500L),
      createRangeMeta(3000L, 4000L)));
  }

  @Test
  public void testIsStreamCreated() {
    assertFalse(isStreamCreated(LifecycleState.UNINIT));
    assertFalse(isStreamCreated(LifecycleState.CREATING));
    assertTrue(isStreamCreated(LifecycleState.CREATED));
    assertTrue(isStreamCreated(LifecycleState.FENCING));
    assertTrue(isStreamCreated(LifecycleState.FENCED));
  }

  @Test
  public void testIsStreamWritable() {
    assertTrue(isStreamWritable(ServingState.WRITABLE));
    assertFalse(isStreamWritable(ServingState.READONLY));
  }

  //
  // Collection API
  //

  @Test
  public void testCreateCreateCollectionRequest() {
    CollectionConfiguration colConf = CollectionConfiguration.newBuilder()
      .setDefaultStreamConf(DEFAULT_STREAM_CONF)
      .build();
    CreateCollectionRequest request = createCreateCollectionRequest(
      name.getMethodName(),
      colConf);
    assertEquals(name.getMethodName(), request.getName());
    assertEquals(colConf, request.getColConf());
  }

  @Test
  public void testCreateDeleteCollectionRequest() {
    DeleteCollectionRequest request = createDeleteCollectionRequest(
      name.getMethodName());
    assertEquals(name.getMethodName(), request.getName());
  }

  @Test
  public void testCreateGetCollectionRequest() {
    GetCollectionRequest request = createGetCollectionRequest(
      name.getMethodName());
    assertEquals(name.getMethodName(), request.getName());
  }

}
