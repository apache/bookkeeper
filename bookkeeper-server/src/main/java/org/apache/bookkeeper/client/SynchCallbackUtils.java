/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Utility for callbacks
 * 
 */
public class SynchCallbackUtils {

    /**
     * Wait for a result. This is convenience method to implement callbacks
     *
     * @param <T>
     * @param future
     * @return
     * @throws InterruptedException
     * @throws BKException
     */
    public static <T> T waitForResult(CompletableFuture<T> future) throws InterruptedException, BKException {
        try {
            return future.get();
        } catch (ExecutionException err) {
            if (err.getCause() instanceof BKException) {
                throw (BKException) err.getCause();
            } else {
                BKException unexpectedConditionException
                    = BKException.create(BKException.Code.UnexpectedConditionException);
                unexpectedConditionException.initCause(err.getCause());
                throw unexpectedConditionException;
            }

        }
    }

    /**
     * Handle the Response Code and transform it to a BKException
     * @param <T>
     * @param rc
     * @param result
     * @param future 
     */
    public static <T> void finish(int rc, T result, CompletableFuture<T> future) {
        if (rc != BKException.Code.OK) {
            future.completeExceptionally(BKException.create(rc).fillInStackTrace());
        } else {
            future.complete(result);
        }
    }

}
