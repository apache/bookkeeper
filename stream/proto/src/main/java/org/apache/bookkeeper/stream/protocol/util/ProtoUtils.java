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
package org.apache.bookkeeper.stream.protocol.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.bookkeeper.common.util.Revisioned;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.bookkeeper.stream.proto.StreamMetadata.LifecycleState;
import org.apache.bookkeeper.stream.proto.StreamMetadata.ServingState;
import org.apache.bookkeeper.stream.proto.StreamName;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.CreateStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.DeleteNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.DeleteStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesRequest;
import org.apache.bookkeeper.stream.proto.storage.GetNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.GetStorageContainerEndpointRequest;
import org.apache.bookkeeper.stream.proto.storage.GetStorageContainerEndpointResponse;
import org.apache.bookkeeper.stream.proto.storage.GetStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.OneStorageContainerEndpointRequest;
import org.apache.bookkeeper.stream.proto.storage.OneStorageContainerEndpointResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerEndpoint;
import org.apache.commons.lang3.tuple.Pair;

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
     * Validate namespace name.
     *
     * @return true if it is a valid namespace name. otherwise false.
     */
    public static boolean validateNamespaceName(String name) {
        return validateStreamName(name);
    }

    /**
     * Validate stream name.
     *
     * <p>follow the rules that dlog uses for validating stream name.
     *
     * @return true if it is a valid namespace name. otherwise false.
     */
    public static boolean validateStreamName(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return false;
        }
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c == 0
                || c == ' '
                || c == '<'
                || c == '>'
                || c > '\u0000' && c < '\u001f'
                || c > '\u007f' && c < '\u009f'
                || c > '\ud800' && c < '\uf8ff'
                || c > '\ufff0' && c < '\uffff') {
                return false;
            }
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

    public static GetActiveRangesRequest createGetActiveRangesRequest(long streamId) {
        return GetActiveRangesRequest.newBuilder()
                .setStreamId(streamId)
                .build();
    }

    public static GetActiveRangesRequest createGetActiveRangesRequest(StreamProperties streamProps) {
        return GetActiveRangesRequest.newBuilder()
                .setStreamId(streamProps.getStreamId())
                .setStreamProps(streamProps)
                .build();
    }

    //
    // Namespace API
    //

    /**
     * Create a {@link CreateNamespaceRequest}.
     *
     * @param nsName namespace name
     * @param nsConf namespace conf
     * @return a create namespace request.
     */
    public static CreateNamespaceRequest createCreateNamespaceRequest(String nsName,
                                                                      NamespaceConfiguration nsConf) {
        return CreateNamespaceRequest.newBuilder()
            .setName(nsName)
            .setNsConf(nsConf)
            .build();
    }

    /**
     * Create a {@link DeleteNamespaceRequest}.
     *
     * @param colName namespace name
     * @return a delete namespace request.
     */
    public static DeleteNamespaceRequest createDeleteNamespaceRequest(String colName) {
        return DeleteNamespaceRequest.newBuilder()
            .setName(colName)
            .build();
    }

    /**
     * Create a {@link GetNamespaceRequest}.
     *
     * @param colName namespace name
     * @return a get namespace request.
     */
    public static GetNamespaceRequest createGetNamespaceRequest(String colName) {
        return GetNamespaceRequest.newBuilder()
            .setName(colName)
            .build();
    }

    //
    // Stream API
    //

    /**
     * Create a {@link CreateStreamRequest}.
     *
     * @param nsName     namespace name
     * @param streamName stream name
     * @param streamConf stream configuration
     * @return a create stream request.
     */
    public static CreateStreamRequest createCreateStreamRequest(String nsName,
                                                                String streamName,
                                                                StreamConfiguration streamConf) {
        return CreateStreamRequest.newBuilder()
            .setNsName(nsName)
            .setName(streamName)
            .setStreamConf(streamConf)
            .build();
    }

    /**
     * Create a {@link GetStreamRequest}.
     *
     * @param nsName     namespace name
     * @param streamName stream name
     * @return a create stream request.
     */
    public static GetStreamRequest createGetStreamRequest(String nsName,
                                                          String streamName) {
        return GetStreamRequest.newBuilder()
            .setStreamName(StreamName.newBuilder()
                .setNamespaceName(nsName)
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
     * @param nsName     namespace name
     * @param streamName stream name
     * @return a create stream request.
     */
    public static DeleteStreamRequest createDeleteStreamRequest(String nsName,
                                                                String streamName) {
        return DeleteStreamRequest.newBuilder()
            .setName(streamName)
            .setNsName(nsName)
            .build();
    }

}
