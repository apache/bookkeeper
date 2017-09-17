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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.bookkeeper.common.util.Revisioned;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.distributedlog.stream.proto.CollectionConfiguration;
import org.apache.distributedlog.stream.proto.RangeProperties;
import org.apache.distributedlog.stream.proto.StreamConfiguration;
import org.apache.distributedlog.stream.proto.StreamMetadata.LifecycleState;
import org.apache.distributedlog.stream.proto.StreamMetadata.ServingState;
import org.apache.distributedlog.stream.proto.StreamName;
import org.apache.distributedlog.stream.proto.rangeservice.CreateCollectionRequest;
import org.apache.distributedlog.stream.proto.rangeservice.CreateStreamRequest;
import org.apache.distributedlog.stream.proto.rangeservice.DeleteCollectionRequest;
import org.apache.distributedlog.stream.proto.rangeservice.DeleteStreamRequest;
import org.apache.distributedlog.stream.proto.rangeservice.GetActiveRangesRequest;
import org.apache.distributedlog.stream.proto.rangeservice.GetCollectionRequest;
import org.apache.distributedlog.stream.proto.rangeservice.GetStorageContainerEndpointRequest;
import org.apache.distributedlog.stream.proto.rangeservice.GetStorageContainerEndpointResponse;
import org.apache.distributedlog.stream.proto.rangeservice.GetStreamRequest;
import org.apache.distributedlog.stream.proto.rangeservice.OneStorageContainerEndpointRequest;
import org.apache.distributedlog.stream.proto.rangeservice.OneStorageContainerEndpointResponse;
import org.apache.distributedlog.stream.proto.rangeservice.StatusCode;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerEndpoint;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerRequest;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerRequest.Type;

/**
 * Protocol related utils.
 */
public class ProtoUtils {

  /**
   * Check if two key ranges overlaps with each other.
   *
   * @param meta1 first key range
   * @param meta2 second key range
   * @return true if two key ranges overlaps
   */
  public static boolean keyRangeOverlaps(RangeProperties meta1, RangeProperties meta2) {
    return keyRangeOverlaps(
      meta1.getStartHashKey(), meta1.getEndHashKey(),
      meta2.getStartHashKey(), meta2.getEndHashKey());
  }

  public static boolean keyRangeOverlaps(Pair<Long, Long> range1,
                                         Pair<Long, Long> range2) {
    return keyRangeOverlaps(range1.getLeft(), range1.getRight(),
      range2.getLeft(), range2.getRight());
  }

  public static boolean keyRangeOverlaps(RangeProperties range1,
                                         Pair<Long, Long> range2) {
    return keyRangeOverlaps(range1.getStartHashKey(), range1.getEndHashKey(),
      range2.getLeft(), range2.getRight());
  }

  public static boolean keyRangeOverlaps(Pair<Long, Long> range1,
                                         RangeProperties range2) {
    return keyRangeOverlaps(range1.getLeft(), range1.getRight(),
      range2.getStartHashKey(), range2.getEndHashKey());
  }

  static boolean keyRangeOverlaps(long startKey1,
                                  long endKey1,
                                  long startKey2,
                                  long endKey2) {
    return endKey2 > startKey1 && startKey2 < endKey1;
  }

  /**
   * Validate collection characters are [a-zA-Z_0-9].
   *
   * @return true if it is a valid collection name. otherwise false.
   */
  public static boolean validateCollectionName(String name) {
    if (StringUtils.isBlank(name)) {
      return false;
    }
    for (int i = 0; i < name.length(); i++) {
      if (Character.isLetterOrDigit(name.charAt(i)) || name.charAt(i) == '_') {
        continue;
      }
      return false;
    }
    return true;
  }

  /**
   * Validate collection characters are [a-zA-Z_0-9].
   *
   * @return true if it is a valid collection name. otherwise false.
   */
  public static boolean validateStreamName(String name) {
    if (StringUtils.isBlank(name)) {
      return false;
    }
    for (int i = 0; i < name.length(); i++) {
      if (Character.isLetterOrDigit(name.charAt(i)) || name.charAt(i) == '_') {
        continue;
      }
      return false;
    }
    return true;
  }

  public static List<RangeProperties> split(long streamId,
                                            int numInitialRanges,
                                            long nextRangeId,
                                            StorageContainerPlacementPolicy placementPolicy) {
    int numRanges = Math.max(2, numInitialRanges);
    if (numRanges % 2 != 0) { // round up to odd number
      numRanges = numRanges + 1;
    }
    long rangeSize = Long.MAX_VALUE / (numRanges / 2);
    long startKey = Long.MIN_VALUE;
    List<RangeProperties> ranges = Lists.newArrayListWithExpectedSize(numRanges);
    for (int idx = 0; idx < numRanges; ++idx) {
      long endKey = startKey + rangeSize;
      if (numRanges - 1 == idx) {
        endKey = Long.MAX_VALUE;
      }
      long rangeId = nextRangeId++;
      RangeProperties props = RangeProperties.newBuilder()
        .setStartHashKey(startKey)
        .setEndHashKey(endKey)
        .setStorageContainerId(placementPolicy.placeStreamRange(streamId, rangeId))
        .setRangeId(rangeId)
        .build();
      startKey = endKey;

      ranges.add(props);
    }
    return ranges;
  }

  /**
   * Check if the stream is created.
   *
   * @param state stream state
   * @return true if the stream is in created state. otherwise, false.
   */
  public static boolean isStreamCreated(LifecycleState state) {
    checkArgument(state != LifecycleState.UNRECOGNIZED);
    return LifecycleState.UNINIT != state
      && LifecycleState.CREATING != state;
  }

  /**
   * Check if the stream is writable.
   *
   * @param state stream state
   * @return true if the stream is writable. otherwise, false.
   */
  public static boolean isStreamWritable(ServingState state) {
    checkArgument(state != ServingState.UNRECOGNIZED);
    return ServingState.WRITABLE == state;
  }

  //
  // Location API
  //

  /**
   * Create a {@link GetStorageContainerEndpointRequest}.
   *
   * @param storageContainers list of storage containers
   * @return a get storage container endpoint request.
   */
  public static GetStorageContainerEndpointRequest createGetStorageContainerEndpointRequest(
    List<Revisioned<Long>> storageContainers) {
    GetStorageContainerEndpointRequest.Builder builder = GetStorageContainerEndpointRequest.newBuilder();
    for (Revisioned<Long> storageContainer : storageContainers) {
      builder.addRequests(
        OneStorageContainerEndpointRequest.newBuilder()
          .setStorageContainer(storageContainer.getValue())
          .setRevision(storageContainer.getRevision()));
    }
    return builder.build();
  }

  /**
   * Create a {@link GetStorageContainerEndpointResponse}.
   *
   * @param endpoints list of storage container endpoints
   * @return a get storage container endpoint response.
   */
  public static GetStorageContainerEndpointResponse createGetStorageContainerEndpointResponse(
    List<StorageContainerEndpoint> endpoints) {
    GetStorageContainerEndpointResponse.Builder builder = GetStorageContainerEndpointResponse.newBuilder();
    builder.setStatusCode(StatusCode.SUCCESS);
    for (StorageContainerEndpoint endpoint : endpoints) {
      builder.addResponses(
        OneStorageContainerEndpointResponse.newBuilder()
          .setStatusCode(StatusCode.SUCCESS)
          .setEndpoint(endpoint));
    }
    return builder.build();
  }


  //
  // Meta Range API
  //

  private static StorageContainerRequest.Builder newScRequestBuilder(long scId, Type type) {
    return StorageContainerRequest.newBuilder()
      .setType(type)
      .setScId(scId);
  }

  public static StorageContainerRequest createGetActiveRangesRequest(long scId, long streamId) {
    return newScRequestBuilder(scId, Type.GET_ACTIVE_RANGES)
      .setGetActiveRangesReq(GetActiveRangesRequest.newBuilder()
        .setStreamId(streamId))
      .build();
  }

  //
  // Collection API
  //

  /**
   * Create a {@link CreateCollectionRequest}.
   *
   * @param colName collection name
   * @param colConf collection conf
   * @return a create collection request.
   */
  public static CreateCollectionRequest createCreateCollectionRequest(String colName,
                                                                      CollectionConfiguration colConf) {
    return CreateCollectionRequest.newBuilder()
      .setName(colName)
      .setColConf(colConf)
      .build();
  }

  /**
   * Create a {@link DeleteCollectionRequest}.
   *
   * @param colName collection name
   * @return a delete collection request.
   */
  public static DeleteCollectionRequest createDeleteCollectionRequest(String colName) {
    return DeleteCollectionRequest.newBuilder()
      .setName(colName)
      .build();
  }

  /**
   * Create a {@link GetCollectionRequest}.
   *
   * @param colName collection name
   * @return a get collection request.
   */
  public static GetCollectionRequest createGetCollectionRequest(String colName) {
    return GetCollectionRequest.newBuilder()
      .setName(colName)
      .build();
  }

  //
  // Stream API
  //

  /**
   * Create a {@link CreateStreamRequest}.
   *
   * @param colName collection name
   * @param streamName stream name
   * @param streamConf stream configuration
   * @return a create stream request.
   */
  public static CreateStreamRequest createCreateStreamRequest(String colName,
                                                              String streamName,
                                                              StreamConfiguration streamConf) {
    return CreateStreamRequest.newBuilder()
      .setColName(colName)
      .setName(streamName)
      .setStreamConf(streamConf)
      .build();
  }

  /**
   * Create a {@link GetStreamRequest}.
   *
   * @param colName collection name
   * @param streamName stream name
   * @return a create stream request.
   */
  public static GetStreamRequest createGetStreamRequest(String colName, String streamName) {
    return GetStreamRequest.newBuilder()
      .setStreamName(StreamName.newBuilder()
        .setColName(colName)
        .setStreamName(streamName))
      .build();
  }

  /**
   * Create a {@link GetStreamRequest}.
   *
   * @param streamId stream id
   * @return a create stream request.
   */
  public static GetStreamRequest createGetStreamRequest(long streamId) {
    return GetStreamRequest.newBuilder()
      .setStreamId(streamId)
      .build();
  }

  /**
   * Create a {@link DeleteStreamRequest}.
   *
   * @param colName collection name
   * @param streamName stream name
   * @return a create stream request.
   */
  public static DeleteStreamRequest createDeleteStreamRequest(String colName, String streamName) {
    return DeleteStreamRequest.newBuilder()
      .setName(streamName)
      .setColName(colName)
      .build();
  }

}
