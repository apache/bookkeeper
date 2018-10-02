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

package org.apache.bookkeeper.clients.utils;

import static org.apache.bookkeeper.common.util.ListenableFutures.fromListenableFuture;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.bookkeeper.common.util.Backoff;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.Retries;

/**
 * Retry Utils.
 */
public class RetryUtils {

    @VisibleForTesting
    static final Predicate<Throwable> DEFAULT_CLIENT_RETRY_PREDICATE =
        cause -> shouldRetryOnException(cause);

    private static boolean shouldRetryOnException(Throwable cause) {
        if (cause instanceof StatusRuntimeException || cause instanceof StatusException) {
            Status status;
            if (cause instanceof StatusException) {
                status = ((StatusException) cause).getStatus();
            } else {
                status = ((StatusRuntimeException) cause).getStatus();
            }
            switch (status.getCode()) {
                case INVALID_ARGUMENT:
                case ALREADY_EXISTS:
                case PERMISSION_DENIED:
                case UNAUTHENTICATED:
                    return false;
                default:
                    return true;
            }
        } else if (cause instanceof RuntimeException) {
            return false;
        } else {
            // storage level exceptions
            return false;
        }
    }

    public static RetryUtils create(Backoff.Policy backoffPolicy,
                                    OrderedScheduler scheduler) {
        return create(DEFAULT_CLIENT_RETRY_PREDICATE, backoffPolicy, scheduler);
    }

    public static RetryUtils create(Predicate<Throwable> retryPredicate,
                                    Backoff.Policy backoffPolicy,
                                    OrderedScheduler scheduler) {
        return new RetryUtils(retryPredicate, backoffPolicy, scheduler);
    }

    private final Predicate<Throwable> retryPredicate;
    private final Backoff.Policy backoffPolicy;
    private final OrderedScheduler scheduler;

    private RetryUtils(Predicate<Throwable> retryPredicate,
                       Backoff.Policy backoffPolicy,
                       OrderedScheduler scheduler) {
        this.retryPredicate = retryPredicate;
        this.backoffPolicy = backoffPolicy;
        this.scheduler = scheduler;
    }

    /**
     * Run the action with retries.
     *
     * @param action action to run
     * @return the result of the action
     */
    public <T> CompletableFuture<T> execute(Supplier<CompletableFuture<T>> action) {
        return Retries.run(
            backoffPolicy.toBackoffs(),
            retryPredicate,
            action,
            scheduler
        );
    }

    /**
     * Run the action with retries.
     *
     * @param action action to run
     * @return the result of the action
     */
    public <T> CompletableFuture<T> executeListenable(Supplier<ListenableFuture<T>> action) {
        return Retries.run(
            backoffPolicy.toBackoffs(),
            retryPredicate,
            () -> fromListenableFuture(action.get()),
            scheduler
        );
    }

}
