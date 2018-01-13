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

package org.apache.distributedlog.stream.storage.impl.metadata;

import static org.apache.distributedlog.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import org.apache.distributedlog.stream.proto.CollectionMetadata;
import org.apache.distributedlog.stream.proto.CollectionProperties;
import org.apache.distributedlog.stream.proto.StreamConfiguration;
import org.apache.distributedlog.stream.proto.StreamProperties;
import org.apache.distributedlog.stream.storage.api.metadata.Collection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test for {@link CollectionImpl}.
 */
public class TestCollectionImpl {

  @Rule
  public TestName name = new TestName();

  private StreamProperties createStreamProperties(long streamId,
                                                  String streamName) {
    return StreamProperties.newBuilder()
      .setStorageContainerId(1L)
      .setStreamId(streamId)
      .setStreamName(streamName)
      .setStreamConf(DEFAULT_STREAM_CONF)
      .build();
  }

  @Test
  public void testCollectionId() {
    long collectionId = System.currentTimeMillis();
    Collection col = CollectionImpl.of(collectionId, name.getMethodName());
    assertEquals(collectionId, col.getId());
  }

  @Test
  public void testCollectionName() {
    long collectionId = System.currentTimeMillis();
    Collection col = CollectionImpl.of(collectionId, name.getMethodName());
    assertEquals(name.getMethodName(), col.getName());
  }

  @Test
  public void testCollectionMetadata() {
    long collectionId = System.currentTimeMillis();
    Collection col = CollectionImpl.of(collectionId, name.getMethodName());
    assertEquals(
      CollectionMetadata.getDefaultInstance(),
      col.getMetadata());
    CollectionMetadata metadata = CollectionMetadata.newBuilder()
      .setProps(
        CollectionProperties.newBuilder()
          .setCollectionName(name.getMethodName())
          .setCollectionId(collectionId)
          .setDefaultStreamConf(
            StreamConfiguration.newBuilder()
              .build())
          .build())
      .build();
    col.setMetadata(metadata);
    assertEquals(metadata, col.getMetadata());
  }

  @Test
  public void testCollectionStreams() {
    long collectionId = System.currentTimeMillis();
    Collection col = CollectionImpl.of(collectionId, name.getMethodName());
    Set<String> streams = col.getStreams();
    assertTrue(streams.isEmpty());
    String name1 = "stream-1";
    String name2 = "stream-2";

    StreamProperties props1 = createStreamProperties(1234L, name1);
    StreamProperties props2 = createStreamProperties(1235L, name2);

    // add stream1 => [stream1]
    assertTrue(col.addStream(name1, props1));
    assertFalse(col.addStream(name1, props2));
    streams = col.getStreams();
    assertEquals(1, streams.size());
    assertTrue(streams.contains(name1));

    // remove stream2 => [stream1]
    assertNull(col.removeStream(name2));
    streams = col.getStreams();
    assertEquals(1, streams.size());
    assertTrue(streams.contains(name1));

    // add stream2 => [stream1, stream2]
    assertTrue(col.addStream(name2, props2));
    streams = col.getStreams();
    assertEquals(2, streams.size());
    assertTrue(streams.contains(name1));
    assertTrue(streams.contains(name2));

    // remove stream1 => [stream2]
    assertNotNull(col.removeStream(name1));
    streams = col.getStreams();
    assertEquals(1, streams.size());
    assertFalse(streams.contains(name1));
    assertTrue(streams.contains(name2));

  }

}
