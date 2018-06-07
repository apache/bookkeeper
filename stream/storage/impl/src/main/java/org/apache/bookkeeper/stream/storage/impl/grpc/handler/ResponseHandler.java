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

package org.apache.bookkeeper.stream.storage.impl.grpc.handler;

import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.function.BiConsumer;

/**
 * Response handler to handle response.
 */
public abstract class ResponseHandler<RespT> implements BiConsumer<RespT, Throwable> {

    protected final StreamObserver<RespT> respObserver;

    protected ResponseHandler(StreamObserver<RespT> respObserver) {
        this.respObserver = respObserver;
    }

    protected abstract RespT createErrorResp(Throwable cause);

    @Override
    public void accept(RespT respT, Throwable cause) {
        if (null != cause) {
            if (cause instanceof StatusRuntimeException
                || cause instanceof StatusException) {
                respObserver.onError(cause);
                return;
            }
            respObserver.onNext(createErrorResp(cause));
        } else {
            respObserver.onNext(respT);
        }
        respObserver.onCompleted();
    }
}
