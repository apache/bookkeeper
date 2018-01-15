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

package org.apache.distributedlog.stream.server.handler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.distributedlog.stream.proto.storage.StatusCode;
import org.apache.distributedlog.stream.proto.storage.StorageContainerResponse;
import org.apache.distributedlog.stream.storage.exceptions.StorageException;
import org.junit.Test;
import org.mockito.stubbing.Answer;

/**
 * Unit test for {@link StorageContainerResponseHandler}.
 */
public class TestStorageContainerResponseHandler {

  @Test
  public void testSuccessResponse() {
    StreamObserver<StorageContainerResponse> observer =
      mock(StreamObserver.class);
    StorageContainerResponseHandler handler = StorageContainerResponseHandler.of(observer);
    StorageContainerResponse response = StorageContainerResponse.newBuilder()
      .setCode(StatusCode.SUCCESS)
      .build();
    handler.accept(response, null);
    verify(observer, times(1)).onNext(response);
    verify(observer, times(1)).onCompleted();
    verify(observer, times(0)).onError(any());
  }

  @Test
  public void testStatusRuntimeException() {
    StreamObserver<StorageContainerResponse> observer =
      mock(StreamObserver.class);
    StorageContainerResponseHandler handler = StorageContainerResponseHandler.of(observer);
    StatusRuntimeException exception = new StatusRuntimeException(Status.NOT_FOUND);
    handler.accept(null, exception);
    verify(observer, times(0)).onNext(any());
    verify(observer, times(0)).onCompleted();
    verify(observer, times(1)).onError(exception);
  }

  @Test
  public void testStatusException() {
    StreamObserver<StorageContainerResponse> observer =
      mock(StreamObserver.class);
    StorageContainerResponseHandler handler = StorageContainerResponseHandler.of(observer);
    StatusException exception = new StatusException(Status.NOT_FOUND);
    handler.accept(null, exception);
    verify(observer, times(0)).onNext(any());
    verify(observer, times(0)).onCompleted();
    verify(observer, times(1)).onError(exception);
  }

  @Test
  public void testInternalError() throws Exception {
    StreamObserver<StorageContainerResponse> observer =
      mock(StreamObserver.class);
    AtomicReference<StorageContainerResponse> responseHolder =
      new AtomicReference<>(null);
    CountDownLatch latch = new CountDownLatch(1);
    doAnswer((Answer<Void>) invocation -> {
      StorageContainerResponse resp = invocation.getArgument(0);
      responseHolder.set(resp);
      latch.countDown();
      return null;
    }).when(observer).onNext(any(StorageContainerResponse.class));
    StorageContainerResponseHandler handler = StorageContainerResponseHandler.of(observer);
    StorageException exception = new StorageException("test-exception");
    handler.accept(null, exception);
    verify(observer, times(1)).onNext(any());
    verify(observer, times(1)).onCompleted();
    verify(observer, times(0)).onError(any());

    latch.await();
    assertNotNull(responseHolder.get());
    assertEquals(
      StatusCode.INTERNAL_SERVER_ERROR,
      responseHolder.get().getCode());
  }

}
