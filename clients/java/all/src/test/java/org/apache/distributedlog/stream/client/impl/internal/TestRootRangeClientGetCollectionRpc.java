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

package org.apache.distributedlog.stream.client.impl.internal;

import static org.apache.distributedlog.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
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
import org.apache.distributedlog.stream.client.exceptions.ClientException;
import org.apache.distributedlog.stream.client.exceptions.CollectionNotFoundException;
import org.apache.distributedlog.stream.client.internal.api.RootRangeClient;
import org.apache.distributedlog.stream.proto.CollectionConfiguration;
import org.apache.distributedlog.stream.proto.CollectionProperties;
import org.apache.distributedlog.stream.proto.rangeservice.GetCollectionRequest;
import org.apache.distributedlog.stream.proto.rangeservice.GetCollectionResponse;
import org.apache.distributedlog.stream.proto.rangeservice.RootRangeServiceGrpc.RootRangeServiceImplBase;
import org.apache.distributedlog.stream.proto.rangeservice.StatusCode;

/**
 * Test Case for {@link RootRangeClientImpl}: CreateCollection.
 */
public class TestRootRangeClientGetCollectionRpc extends RootRangeClientImplTestBase {

  private long colId;
  private String colName;
  private CollectionProperties colProps;
  private static final CollectionConfiguration colConf = CollectionConfiguration.newBuilder()
    .setDefaultStreamConf(DEFAULT_STREAM_CONF)
    .build();

  @Override
  protected void doSetup() throws Exception {
    super.doSetup();

    this.colId = System.currentTimeMillis();
    this.colName = testName.getMethodName();
    this.colProps = CollectionProperties.newBuilder()
      .setCollectionId(colId)
      .setCollectionName(colName)
      .setDefaultStreamConf(DEFAULT_STREAM_CONF)
      .build();
  }

  //
  // Test Client Operations
  //

  //
  // Collection API
  //


  @Override
  protected RootRangeServiceImplBase createRootRangeServiceForSuccess() {
    return new RootRangeServiceImplBase() {
      @Override
      public void getCollection(GetCollectionRequest request,
                                StreamObserver<GetCollectionResponse> responseObserver) {
        responseObserver.onNext(GetCollectionResponse.newBuilder()
          .setCode(StatusCode.SUCCESS)
          .setColProps(colProps)
          .build());
        responseObserver.onCompleted();
      }
    };
  }

  @Override
  protected void verifySuccess(RootRangeClient rootRangeClient) throws Exception {
    CompletableFuture<CollectionProperties> getFuture = rootRangeClient.getCollection(colName);
    assertTrue(colProps == getFuture.get());
  }


  @Override
  protected RootRangeServiceImplBase createRootRangeServiceForRequestFailure() {
    return new RootRangeServiceImplBase() {
      @Override
      public void getCollection(GetCollectionRequest request,
                                StreamObserver<GetCollectionResponse> responseObserver) {
        responseObserver.onNext(GetCollectionResponse.newBuilder()
          .setCode(StatusCode.COLLECTION_NOT_FOUND)
          .build());
        responseObserver.onCompleted();
      }
    };
  }

  @Override
  protected void verifyRequestFailure(RootRangeClient rootRangeClient) throws Exception {
    CompletableFuture<CollectionProperties> getFuture = rootRangeClient.getCollection(colName);
    try {
      getFuture.get();
      fail("Should fail on rpc failure");
    } catch (ExecutionException ee) {
      assertNotNull(ee.getCause());
      assertTrue(ee.getCause() instanceof CollectionNotFoundException);
    }
  }

  @Override
  protected RootRangeServiceImplBase createRootRangeServiceForRpcFailure() {
    return new RootRangeServiceImplBase() {
      @Override
      public void getCollection(GetCollectionRequest request,
                                StreamObserver<GetCollectionResponse> responseObserver) {
        responseObserver.onError(new StatusRuntimeException(Status.INTERNAL));
      }
    };
  }

  @Override
  protected void verifyRpcFailure(RootRangeClient rootRangeClient) throws Exception {
    CompletableFuture<CollectionProperties> getFuture = rootRangeClient.getCollection(colName);
    try {
      getFuture.get();
      fail("Should fail on rpc failure");
    } catch (ExecutionException ee) {
      assertNotNull(ee.getCause());
      assertTrue(ee.getCause() instanceof StatusRuntimeException);
      StatusRuntimeException se = (StatusRuntimeException) ee.getCause();
      assertEquals(Status.INTERNAL, se.getStatus());
    }
  }

  @Override
  protected void verifyChannelFailure(IOException expectedException, RootRangeClient rootRangeClient) throws Exception {
    CompletableFuture<CollectionProperties> createFuture = rootRangeClient.createCollection(colName, colConf);
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
