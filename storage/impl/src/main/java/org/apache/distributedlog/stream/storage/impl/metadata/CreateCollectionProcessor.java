/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.distributedlog.stream.storage.impl.metadata;

import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.stream.proto.rangeservice.CreateCollectionRequest;
import org.apache.distributedlog.stream.proto.rangeservice.CreateCollectionResponse;
import org.apache.distributedlog.stream.proto.rangeservice.StatusCode;
import org.apache.distributedlog.stream.storage.impl.AsyncOperationProcessor;

/**
 * The operation process for creating collection.
 */
class CreateCollectionProcessor
    extends AsyncOperationProcessor<CreateCollectionRequest, CreateCollectionResponse, RootRangeStoreImpl> {

  public static CreateCollectionProcessor of() {
    return INSTANCE;
  }

  private static final CreateCollectionProcessor INSTANCE = new CreateCollectionProcessor();

  private CreateCollectionProcessor() {}

  @Override
  protected StatusCode verifyRequest(RootRangeStoreImpl state,
                                     CreateCollectionRequest request) {
    return state.verifyCreateCollectionRequest(request);
  }

  @Override
  protected CreateCollectionResponse failRequest(StatusCode code) {
    return CreateCollectionResponse.newBuilder()
      .setCode(code)
      .build();
  }

  @Override
  protected CompletableFuture<CreateCollectionResponse> doProcessRequest(RootRangeStoreImpl state,
                                                                           CreateCollectionRequest request) {
    return state.doProcessCreateCollectionRequest(request);
  }
}
