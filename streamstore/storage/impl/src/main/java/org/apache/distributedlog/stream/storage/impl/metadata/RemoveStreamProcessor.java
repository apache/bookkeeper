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

import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.stream.proto.rangeservice.RemoveStreamRequest;
import org.apache.distributedlog.stream.proto.rangeservice.StatusCode;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerResponse;
import org.apache.distributedlog.stream.storage.impl.AsyncOperationProcessor;

/**
 * Processor to process removing stream from a storage container.
 */
public class RemoveStreamProcessor
    extends AsyncOperationProcessor<RemoveStreamRequest, StorageContainerResponse, MetaRangeStoreImpl> {

  public static RemoveStreamProcessor of() {
    return INSTANCE;
  }

  private static final RemoveStreamProcessor INSTANCE = new RemoveStreamProcessor();

  private RemoveStreamProcessor() {}

  @Override
  protected StatusCode verifyRequest(MetaRangeStoreImpl state, RemoveStreamRequest request) {
    return state.verifyRemoveStreamRequest(request);
  }

  @Override
  protected StorageContainerResponse failRequest(StatusCode code) {
    return StorageContainerResponse.newBuilder()
      .setCode(code)
      .build();
  }

  @Override
  protected CompletableFuture<StorageContainerResponse> doProcessRequest(MetaRangeStoreImpl state,
                                                                         RemoveStreamRequest request) {
    return state.doProcessRemoveStreamRequest(request);
  }
}
