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

import io.grpc.stub.StreamObserver;
import org.apache.distributedlog.stream.proto.storage.StatusCode;
import org.apache.distributedlog.stream.proto.storage.StorageContainerResponse;

/**
 * Response handler to handle storage container response.
 */
public class StorageContainerResponseHandler extends ResponseHandler<StorageContainerResponse> {

  public static StorageContainerResponseHandler of(StreamObserver<StorageContainerResponse> respObserver) {
    return new StorageContainerResponseHandler(respObserver);
  }

  private StorageContainerResponseHandler(StreamObserver<StorageContainerResponse> respObserver) {
    super(respObserver);
  }

  @Override
  protected StorageContainerResponse createErrorResp(Throwable cause) {
    return StorageContainerResponse.newBuilder()
      .setCode(StatusCode.INTERNAL_SERVER_ERROR)
      .build();
  }
}
