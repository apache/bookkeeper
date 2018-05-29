/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.common.util;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.createFuture;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Utils to convert {@link com.google.common.util.concurrent.ListenableFuture} to
 * java {@link java.util.concurrent.CompletableFuture}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ListenableFutures {

    /**
     * Convert a {@link ListenableFuture} to a {@link CompletableFuture}.
     *
     * @param listenableFuture listenable future to convert.
     * @return the completable future.
     */
    public static <T> CompletableFuture<T> fromListenableFuture(ListenableFuture<T> listenableFuture) {
        return fromListenableFuture(listenableFuture, Function.identity());
    }

    /**
     * Convert a {@link ListenableFuture} to a {@link CompletableFuture} and do a transformation.
     *
     * @param listenableFuture listenable future
     * @param mapFn            a map function that transform results
     * @return the completable future after transformation.
     */
    public static <T, R> CompletableFuture<R> fromListenableFuture(
        ListenableFuture<T> listenableFuture,
        Function<? super T, ? extends R> mapFn) {
        CompletableFuture<R> completableFuture = createFuture();
        Futures.addCallback(listenableFuture, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                try {
                    R uResult = mapFn.apply(result);
                    completableFuture.complete(uResult);
                } catch (Exception e) {
                    completableFuture.completeExceptionally(e);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                completableFuture.completeExceptionally(t);
            }
        });
        return completableFuture;
    }

    /**
     * Convert a {@link ListenableFuture} to a {@link CompletableFuture} and do a transformation.
     *
     * @param listenableFuture listenable future
     * @param mapFn            a map function that transform results
     * @return the completable future after transformation.
     */
    public static <T, R> CompletableFuture<R> fromListenableFuture(
        ListenableFuture<T> listenableFuture,
        ExceptionalFunction<? super T, ? extends R> mapFn) {
        CompletableFuture<R> completableFuture = createFuture();
        Futures.addCallback(listenableFuture, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                try {
                    R uResult = mapFn.apply(result);
                    completableFuture.complete(uResult);
                } catch (Exception e) {
                    completableFuture.completeExceptionally(e);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                completableFuture.completeExceptionally(t);
            }
        });
        return completableFuture;
    }
}
