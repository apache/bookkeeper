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
import org.apache.distributedlog.stream.proto.rangeservice.DeleteCollectionRequest;
import org.apache.distributedlog.stream.proto.rangeservice.DeleteCollectionResponse;
import org.apache.distributedlog.stream.proto.rangeservice.StatusCode;
import org.apache.distributedlog.stream.storage.impl.AsyncOperationProcessor;

/**
 * The operation process for creating collection.
 */
class DeleteCollectionProcessor
    extends AsyncOperationProcessor<DeleteCollectionRequest, DeleteCollectionResponse, RootRangeStoreImpl> {

  public static DeleteCollectionProcessor of() {
    return INSTANCE;
  }

  private static final DeleteCollectionProcessor INSTANCE = new DeleteCollectionProcessor();

  private DeleteCollectionProcessor() {}

  @Override
  protected StatusCode verifyRequest(RootRangeStoreImpl state,
                                     DeleteCollectionRequest request) {
    return state.verifyDeleteCollectionRequest(request);
  }

  @Override
  protected DeleteCollectionResponse failRequest(StatusCode code) {
    return DeleteCollectionResponse.newBuilder()
      .setCode(code)
      .build();
  }

  @Override
  protected CompletableFuture<DeleteCollectionResponse> doProcessRequest(RootRangeStoreImpl state,
                                                                         DeleteCollectionRequest request) {
    return state.doProcessDeleteCollectionRequest(request);
  }
}
