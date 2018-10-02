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
package org.apache.bookkeeper.clients.impl.internal;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Maps;
import java.util.List;
import java.util.TreeMap;
import org.apache.bookkeeper.clients.exceptions.ClientException;
import org.apache.bookkeeper.clients.exceptions.InternalServerException;
import org.apache.bookkeeper.clients.exceptions.InvalidNamespaceNameException;
import org.apache.bookkeeper.clients.exceptions.InvalidStreamNameException;
import org.apache.bookkeeper.clients.exceptions.NamespaceExistsException;
import org.apache.bookkeeper.clients.exceptions.NamespaceNotFoundException;
import org.apache.bookkeeper.clients.exceptions.StorageContainerException;
import org.apache.bookkeeper.clients.exceptions.StreamExistsException;
import org.apache.bookkeeper.clients.exceptions.StreamNotFoundException;
import org.apache.bookkeeper.clients.impl.internal.api.HashStreamRanges;
import org.apache.bookkeeper.common.util.ExceptionalFunction;
import org.apache.bookkeeper.stream.proto.RangeKeyType;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesResponse;
import org.apache.bookkeeper.stream.proto.storage.GetStorageContainerEndpointResponse;
import org.apache.bookkeeper.stream.proto.storage.OneStorageContainerEndpointResponse;
import org.apache.bookkeeper.stream.proto.storage.RelatedRanges;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;

/**
 * Utils for converting protocol related data structures to internal data structures.
 */
public final class ProtocolInternalUtils {

    private ProtocolInternalUtils() {
    }

    public static HashStreamRanges createActiveRanges(GetActiveRangesResponse response) {
        TreeMap<Long, RangeProperties> ranges = Maps.newTreeMap();
        long lastEndKey = Long.MIN_VALUE;
        for (RelatedRanges rr : response.getRangesList()) {
            RangeProperties range = rr.getProps();
            long startKey = range.getStartHashKey();
            long endKey = range.getEndHashKey();
            checkState(
                lastEndKey == startKey,
                "Invalid range key found : expected = %s, actual = %s", lastEndKey, startKey);
            ranges.put(startKey, range);
            lastEndKey = endKey;
        }
        checkState(
            Long.MAX_VALUE == lastEndKey,
            "Missing key range [%s - %s)", lastEndKey, Long.MAX_VALUE);
        checkState(
            ranges.size() > 0,
            "No active range found");
        return HashStreamRanges.ofHash(
            RangeKeyType.HASH,
            ranges);
    }

    static final ExceptionalFunction<GetStorageContainerEndpointResponse, List<OneStorageContainerEndpointResponse>>
        GetStorageContainerEndpointsFunction = response -> {
        if (StatusCode.SUCCESS == response.getStatusCode()) {
            return response.getResponsesList();
        }
        throw new StorageContainerException(
            response.getStatusCode(),
            "Fail to get storage container endpoints");
    };

    //
    // Exceptions
    //

    public static Throwable createRootRangeException(String streamName, StatusCode statusCode) {
        switch (statusCode) {
            case INVALID_NAMESPACE_NAME:
                return new InvalidNamespaceNameException(streamName);
            case NAMESPACE_EXISTS:
                return new NamespaceExistsException(streamName);
            case NAMESPACE_NOT_FOUND:
                return new NamespaceNotFoundException(streamName);
            case INVALID_STREAM_NAME:
                return new InvalidStreamNameException(streamName);
            case STREAM_EXISTS:
                return new StreamExistsException(streamName);
            case STREAM_NOT_FOUND:
                return new StreamNotFoundException(streamName);
            default:
                return new ClientException("fail to access its root range : code = " + statusCode);
        }
    }

    public static Exception createMetaRangeException(String name,
                                                     StatusCode statusCode) {
        switch (statusCode) {
            case STREAM_EXISTS:
                return new StreamExistsException(name);
            case STREAM_NOT_FOUND:
                return new StreamNotFoundException(name);
            default:
                return new InternalServerException("Encountered internal server exception on stream "
                    + name + " : code = " + statusCode);
        }
    }

}
