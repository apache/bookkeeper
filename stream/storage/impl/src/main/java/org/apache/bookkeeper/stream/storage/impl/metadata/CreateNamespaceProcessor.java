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

package org.apache.bookkeeper.stream.storage.impl.metadata;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.storage.impl.AsyncOperationProcessor;

/**
 * The operation process for creating namespace.
 */
class CreateNamespaceProcessor
    extends AsyncOperationProcessor<CreateNamespaceRequest, CreateNamespaceResponse, RootRangeStoreImpl> {

    public static CreateNamespaceProcessor of() {
        return INSTANCE;
    }

    private static final CreateNamespaceProcessor INSTANCE = new CreateNamespaceProcessor();

    private CreateNamespaceProcessor() {
    }

    @Override
    protected StatusCode verifyRequest(RootRangeStoreImpl state,
                                       CreateNamespaceRequest request) {
        return state.verifyCreateNamespaceRequest(request);
    }

    @Override
    protected CreateNamespaceResponse failRequest(StatusCode code) {
        return CreateNamespaceResponse.newBuilder()
            .setCode(code)
            .build();
    }

    @Override
    protected CompletableFuture<CreateNamespaceResponse> doProcessRequest(RootRangeStoreImpl state,
                                                                          CreateNamespaceRequest request) {
        return state.doProcessCreateNamespaceRequest(request);
    }
}
