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

package org.apache.bookkeeper.stream.storage.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;

/**
 * Operation Processor.
 */
public abstract class AsyncOperationProcessor<ReqT, RespT, StateT> {

    public CompletableFuture<RespT> process(StateT state,
                                            ReqT request,
                                            ScheduledExecutorService executor) {
        CompletableFuture<RespT> future = FutureUtils.createFuture();
        executor.submit(() -> processRequest(
            future,
            state,
            request));
        return future;
    }

    protected abstract StatusCode verifyRequest(StateT state,
                                                ReqT request);

    protected abstract RespT failRequest(StatusCode code);

    protected abstract CompletableFuture<RespT> doProcessRequest(StateT state, ReqT request);

    protected void processRequest(CompletableFuture<RespT> future,
                                  StateT state,
                                  ReqT request) {
        try {
            StatusCode code = verifyRequest(state, request);
            if (StatusCode.SUCCESS != code) {
                future.complete(failRequest(code));
            } else {
                doProcessRequest(state, request).whenComplete((value, causeB) -> {
                    if (null == causeB) {
                        future.complete(value);
                    } else {
                        future.completeExceptionally(causeB);
                    }
                });
            }
        } catch (Throwable cause) {
            future.completeExceptionally(cause);
        }
    }

}
