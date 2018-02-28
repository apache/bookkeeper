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

package org.apache.bookkeeper.common.util;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.exceptions.ObjectClosedException;

/**
 * Utils for exceptions.
 */
public final class ExceptionUtils {

    private ExceptionUtils() {
    }

    /**
     * Execute the callable {@code callable}.
     *
     * <p>If the object {@code componentName} is already closed ({@code isClosed}), it will be
     * failed immediately without calling {@code callable}.
     *
     * @param componentName the component name
     * @param isClosed      predicate to check whether the component is closed or not.
     * @param callable      the callable to execute
     * @param <T>           result type
     * @return the future represents the result.
     */
    public static <T> CompletableFuture<T> callAndHandleClosedAsync(String componentName,
                                                                    boolean isClosed,
                                                                    CompletableRunnable<T> callable) {
        CompletableFuture<T> future = FutureUtils.createFuture();
        if (isClosed) {
            future.completeExceptionally(new ObjectClosedException(componentName));
        } else {
            callable.run(future);
        }
        return future;
    }

    /**
     * Convert a {@link Throwable} to an {@link IOException}.
     *
     * @param cause reason to fail
     * @return a converted {@link IOException}.
     */
    public static IOException toIOException(Throwable cause) {
        if (cause instanceof IOException) {
            return (IOException) cause;
        } else if (cause instanceof ExecutionException || cause instanceof CompletionException) {
            return toIOException(cause.getCause());
        } else {
            return new IOException(cause);
        }
    }

}
