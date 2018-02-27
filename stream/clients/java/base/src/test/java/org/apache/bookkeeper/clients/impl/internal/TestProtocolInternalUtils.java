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

import static org.apache.bookkeeper.clients.impl.internal.ProtocolInternalUtils.createActiveRanges;
import static org.apache.bookkeeper.clients.impl.internal.ProtocolInternalUtils.createMetaRangeException;
import static org.apache.bookkeeper.clients.impl.internal.ProtocolInternalUtils.createRootRangeException;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_RANGE_ID;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createGetStorageContainerEndpointRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createGetStorageContainerEndpointResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.TreeMap;
import org.apache.bookkeeper.clients.exceptions.ClientException;
import org.apache.bookkeeper.clients.exceptions.InvalidNamespaceNameException;
import org.apache.bookkeeper.clients.exceptions.InvalidStreamNameException;
import org.apache.bookkeeper.clients.exceptions.NamespaceExistsException;
import org.apache.bookkeeper.clients.exceptions.NamespaceNotFoundException;
import org.apache.bookkeeper.clients.exceptions.StreamExistsException;
import org.apache.bookkeeper.clients.exceptions.StreamNotFoundException;
import org.apache.bookkeeper.clients.impl.internal.api.HashStreamRanges;
import org.apache.bookkeeper.common.util.Revisioned;
import org.apache.bookkeeper.stream.proto.RangeKeyType;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesResponse;
import org.apache.bookkeeper.stream.proto.storage.GetStorageContainerEndpointRequest;
import org.apache.bookkeeper.stream.proto.storage.GetStorageContainerEndpointResponse;
import org.apache.bookkeeper.stream.proto.storage.OneStorageContainerEndpointRequest;
import org.apache.bookkeeper.stream.proto.storage.OneStorageContainerEndpointResponse;
import org.apache.bookkeeper.stream.proto.storage.RelatedRanges;
import org.apache.bookkeeper.stream.proto.storage.RelationType;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerEndpoint;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test for {@link ProtocolInternalUtils}.
 */
public class TestProtocolInternalUtils {

    @Rule
    public final TestName name = new TestName();

    //
    // Test Meta KeyRange Server Requests
    //

    @Test
    public void testCreateActiveRanges() {
        GetActiveRangesResponse.Builder responseBuilder = GetActiveRangesResponse.newBuilder();
        responseBuilder.addRanges(
            RelatedRanges.newBuilder()
                .setProps(RangeProperties.newBuilder()
                    .setStartHashKey(Long.MIN_VALUE)
                    .setEndHashKey(0L)
                    .setRangeId(1L)
                    .setStorageContainerId(1L))
                .setType(RelationType.PARENTS)
                .addRelatedRanges(INVALID_RANGE_ID)
        ).addRanges(
            RelatedRanges.newBuilder()
                .setProps(RangeProperties.newBuilder()
                    .setStartHashKey(0L)
                    .setEndHashKey(Long.MAX_VALUE)
                    .setRangeId(2L)
                    .setStorageContainerId(2L))
                .setType(RelationType.PARENTS)
                .addRelatedRanges(INVALID_RANGE_ID));
        GetActiveRangesResponse response = responseBuilder.build();
        HashStreamRanges hsr = createActiveRanges(response);
        TreeMap<Long, RangeProperties> activeRanges = Maps.newTreeMap();
        activeRanges.put(Long.MIN_VALUE, response.getRanges(0).getProps());
        activeRanges.put(0L, response.getRanges(1).getProps());
        HashStreamRanges expectedHSR = HashStreamRanges.ofHash(
            RangeKeyType.HASH,
            activeRanges);
        assertEquals(expectedHSR, hsr);
        assertEquals(2L, hsr.getMaxRangeId());
    }

    @Test
    public void testCreateActiveRangesInvalidKeyRange() {
        GetActiveRangesResponse.Builder responseBuilder = GetActiveRangesResponse.newBuilder();
        responseBuilder.addRanges(
            RelatedRanges.newBuilder()
                .setProps(RangeProperties.newBuilder()
                    .setStartHashKey(Long.MIN_VALUE)
                    .setEndHashKey(0L)
                    .setRangeId(1L)
                    .setStorageContainerId(1L))
                .setType(RelationType.PARENTS)
                .addRelatedRanges(INVALID_RANGE_ID)
        ).addRanges(
            RelatedRanges.newBuilder()
                .setProps(RangeProperties.newBuilder()
                    .setStartHashKey(1L)
                    .setEndHashKey(Long.MAX_VALUE)
                    .setRangeId(2L)
                    .setStorageContainerId(2L))
                .setType(RelationType.PARENTS)
                .addRelatedRanges(INVALID_RANGE_ID));
        try {
            createActiveRanges(responseBuilder.build());
            fail("Should fail with invalid key range");
        } catch (IllegalStateException ise) {
            assertEquals(
                String.format("Invalid range key found : expected = %d, actual = %d", 0L, 1L),
                ise.getMessage());
        }
    }

    @Test
    public void testCreateActiveRangesMissingKeyRange() {
        GetActiveRangesResponse.Builder responseBuilder = GetActiveRangesResponse.newBuilder();
        responseBuilder.addRanges(
            RelatedRanges.newBuilder()
                .setProps(RangeProperties.newBuilder()
                    .setStartHashKey(Long.MIN_VALUE)
                    .setEndHashKey(0L)
                    .setRangeId(1L)
                    .setStorageContainerId(1L))
                .setType(RelationType.PARENTS)
                .addRelatedRanges(INVALID_RANGE_ID)
        ).addRanges(
            RelatedRanges.newBuilder()
                .setProps(RangeProperties.newBuilder()
                    .setStartHashKey(0L)
                    .setEndHashKey(1234L)
                    .setRangeId(2L)
                    .setStorageContainerId(2L))
                .setType(RelationType.PARENTS)
                .addRelatedRanges(INVALID_RANGE_ID));
        try {
            createActiveRanges(responseBuilder.build());
            fail("Should fail with missing key range");
        } catch (IllegalStateException ise) {
            assertEquals(
                String.format("Missing key range [%d - %d)", 1234L, Long.MAX_VALUE),
                ise.getMessage());
        }
    }

    //
    // Test Location Server Requests
    //

    @Test
    public void testCreateGetStorageContainerEndpointRequest() {
        List<Revisioned<Long>> scs = Lists.newArrayList(
            Revisioned.of(1000L, 1L),
            Revisioned.of(2000L, 2L),
            Revisioned.of(3000L, 3L));
        GetStorageContainerEndpointRequest request = createGetStorageContainerEndpointRequest(scs);
        assertEquals(3, request.getRequestsCount());
        int i = 1;
        for (OneStorageContainerEndpointRequest oneRequest : request.getRequestsList()) {
            assertEquals(1000L * i, oneRequest.getStorageContainer());
            assertEquals(1L * i, oneRequest.getRevision());
            ++i;
        }
    }

    @Test
    public void testCreateStorageContainerEndpointResponse() {
        List<StorageContainerEndpoint> endpoints = Lists.newArrayList(
            StorageContainerEndpoint.newBuilder().setStorageContainerId(1L).build(),
            StorageContainerEndpoint.newBuilder().setStorageContainerId(2L).build(),
            StorageContainerEndpoint.newBuilder().setStorageContainerId(3L).build());
        GetStorageContainerEndpointResponse response = createGetStorageContainerEndpointResponse(endpoints);
        assertEquals(3, response.getResponsesCount());
        int i = 0;
        for (OneStorageContainerEndpointResponse oneResp : response.getResponsesList()) {
            assertEquals(StatusCode.SUCCESS, oneResp.getStatusCode());
            assertTrue(endpoints.get(i) == oneResp.getEndpoint());
            ++i;
        }
    }

    //
    // Test Exceptions related utils
    //

    @Test
    public void testCreateRootRangeException() {
        String name = "test-create-root-range-exception";
        // stream exists exception
        Throwable cause1 = createRootRangeException(name, StatusCode.STREAM_EXISTS);
        assertTrue(cause1 instanceof StreamExistsException);
        StreamExistsException see = (StreamExistsException) cause1;
        // stream not found
        Throwable cause2 = createRootRangeException(name, StatusCode.STREAM_NOT_FOUND);
        assertTrue(cause2 instanceof StreamNotFoundException);
        StreamNotFoundException snfe = (StreamNotFoundException) cause2;
        // invalid stream name
        Throwable invalidStreamNameCause = createRootRangeException(name, StatusCode.INVALID_STREAM_NAME);
        assertTrue(invalidStreamNameCause instanceof InvalidStreamNameException);
        InvalidStreamNameException isne = (InvalidStreamNameException) invalidStreamNameCause;
        // failure
        Throwable cause3 = createRootRangeException(name, StatusCode.FAILURE);
        ClientException se = (ClientException) cause3;
        assertEquals("fail to access its root range : code = " + StatusCode.FAILURE,
            se.getMessage());
        // namespace exists exception
        Throwable cause5 = createRootRangeException(name, StatusCode.NAMESPACE_EXISTS);
        assertTrue(cause5 instanceof NamespaceExistsException);
        // namespace not-found exception
        Throwable cause6 = createRootRangeException(name, StatusCode.NAMESPACE_NOT_FOUND);
        assertTrue(cause6 instanceof NamespaceNotFoundException);
        // invalid namespace name
        Throwable cause7 = createRootRangeException(name, StatusCode.INVALID_NAMESPACE_NAME);
        assertTrue(cause7 instanceof InvalidNamespaceNameException);
    }

    @Test
    public void testMetaRangeException() {
        String name = "test-meta-range-exception";

        // stream exists
        Throwable existCause = createMetaRangeException(name, StatusCode.STREAM_EXISTS);
        assertTrue(existCause instanceof StreamExistsException);

        // stream not found
        Throwable notFoundCause = createMetaRangeException(name, StatusCode.STREAM_NOT_FOUND);
        assertTrue(notFoundCause instanceof StreamNotFoundException);
    }

}
