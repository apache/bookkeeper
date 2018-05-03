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

package org.apache.bookkeeper.clients.utils;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.CompletableFuture;

/**
 * RPC related utils.
 */
public class RpcUtils {

    /**
     * Function to create request.
     */
    @FunctionalInterface
    public interface CreateRequestFunc<ReqT> {
        ReqT apply();
    }

    /**
     * Function to process request.
     */
    @FunctionalInterface
    public interface ProcessRequestFunc<ReqT, RespT, ServiceT> {
        ListenableFuture<RespT> process(ServiceT service, ReqT req);
    }

    /**
     * Function to process response.
     */
    @FunctionalInterface
    public interface ProcessResponseFunc<RespT, T> {
        void process(RespT resp, CompletableFuture<T> resultFuture);
    }

    public static boolean isContainerNotFound(Throwable cause) {
        if (cause instanceof StatusRuntimeException) {
            return Status.NOT_FOUND ==  ((StatusRuntimeException) cause).getStatus();
        } else if (cause instanceof StatusException) {
            return Status.NOT_FOUND ==  ((StatusException) cause).getStatus();
        } else {
            return false;
        }
    }

    public static <T, ReqT, RespT, ServiceT> void processRpc(
        ServiceT service,
        CompletableFuture<T> result,
        CreateRequestFunc<ReqT> createRequestFunc,
        ProcessRequestFunc<ReqT, RespT, ServiceT> processRequestFunc,
        ProcessResponseFunc<RespT, T> processResponseFunc) {
        ReqT request = createRequestFunc.apply();
        ListenableFuture<RespT> resultFuture =
            processRequestFunc.process(service, request);
        Futures.addCallback(
            resultFuture,
            new FutureCallback<RespT>() {
                @Override
                public void onSuccess(RespT resp) {
                    processResponseFunc.process(resp, result);
                }

                @Override
                public void onFailure(Throwable throwable) {
                    GrpcUtils.processRpcException(throwable, result);
                }
            }
        );
    }

}
