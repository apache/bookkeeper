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

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.clients.exceptions.ClientException;
import org.apache.bookkeeper.clients.exceptions.NamespaceNotFoundException;
import org.apache.bookkeeper.clients.impl.internal.api.RootRangeClient;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.NamespaceProperties;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.RootRangeServiceGrpc.RootRangeServiceImplBase;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;

/**
 * Test Case for {@link RootRangeClientImpl}: CreateNamespace.
 */
public class TestRootRangeClientCreateNamespaceRpc extends RootRangeClientImplTestBase {

    private long colId;
    private String colName;
    private NamespaceProperties colProps;
    private static final NamespaceConfiguration colConf = NamespaceConfiguration.newBuilder()
        .setDefaultStreamConf(DEFAULT_STREAM_CONF)
        .build();

    @Override
    protected void doSetup() throws Exception {
        super.doSetup();

        this.colId = System.currentTimeMillis();
        this.colName = testName.getMethodName();
        this.colProps = NamespaceProperties.newBuilder()
            .setNamespaceId(colId)
            .setNamespaceName(colName)
            .setDefaultStreamConf(DEFAULT_STREAM_CONF)
            .build();
    }

    //
    // Test StorageClient Operations
    //

    //
    // Namespace API
    //


    @Override
    protected RootRangeServiceImplBase createRootRangeServiceForSuccess() {
        return new RootRangeServiceImplBase() {
            @Override
            public void createNamespace(CreateNamespaceRequest request,
                                        StreamObserver<CreateNamespaceResponse> responseObserver) {
                responseObserver.onNext(CreateNamespaceResponse.newBuilder()
                    .setCode(StatusCode.SUCCESS)
                    .setNsProps(colProps)
                    .build());
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    protected void verifySuccess(RootRangeClient rootRangeClient) throws Exception {
        CompletableFuture<NamespaceProperties> createFuture = rootRangeClient.createNamespace(colName, colConf);
        assertTrue(colProps == createFuture.get());
    }


    @Override
    protected RootRangeServiceImplBase createRootRangeServiceForRequestFailure() {
        return new RootRangeServiceImplBase() {
            @Override
            public void createNamespace(CreateNamespaceRequest request,
                                        StreamObserver<CreateNamespaceResponse> responseObserver) {
                responseObserver.onNext(CreateNamespaceResponse.newBuilder()
                    .setCode(StatusCode.NAMESPACE_NOT_FOUND)
                    .build());
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    protected void verifyRequestFailure(RootRangeClient rootRangeClient) throws Exception {
        CompletableFuture<NamespaceProperties> createFuture = rootRangeClient.createNamespace(colName, colConf);
        try {
            createFuture.get();
            fail("Should fail on rpc failure");
        } catch (ExecutionException ee) {
            assertNotNull(ee.getCause());
            assertTrue(ee.getCause() instanceof NamespaceNotFoundException);
        }
    }

    @Override
    protected RootRangeServiceImplBase createRootRangeServiceForRpcFailure() {
        return new RootRangeServiceImplBase() {
            @Override
            public void createNamespace(CreateNamespaceRequest request,
                                        StreamObserver<CreateNamespaceResponse> responseObserver) {
                responseObserver.onError(new StatusRuntimeException(Status.INTERNAL));
            }
        };
    }

    @Override
    protected void verifyRpcFailure(RootRangeClient rootRangeClient) throws Exception {
        CompletableFuture<NamespaceProperties> createFuture = rootRangeClient.createNamespace(colName, colConf);
        try {
            createFuture.get();
            fail("Should fail on rpc failure");
        } catch (ExecutionException ee) {
            assertNotNull(ee.getCause());
            assertTrue(ee.getCause() instanceof StatusRuntimeException);
            StatusRuntimeException se = (StatusRuntimeException) ee.getCause();
            assertEquals(Status.INTERNAL, se.getStatus());
        }
    }

    @Override
    protected void verifyChannelFailure(IOException expectedException, RootRangeClient rootRangeClient)
            throws Exception {
        CompletableFuture<NamespaceProperties> createFuture = rootRangeClient.createNamespace(colName, colConf);
        try {
            createFuture.get();
            fail("Should fail on creating stream");
        } catch (ExecutionException ee) {
            assertNotNull(ee.getCause());
            assertTrue(ee.getCause() instanceof ClientException);
            ClientException zse = (ClientException) ee.getCause();
            assertNotNull(zse.getCause());
            assertTrue(expectedException == zse.getCause());
        }
    }

}
