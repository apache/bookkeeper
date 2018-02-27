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

import java.util.concurrent.CompletableFuture;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

/**
 * A common implementation for {@link AutoAsyncCloseable}.
 */
@Slf4j
public abstract class AbstractAutoAsyncCloseable implements AutoAsyncCloseable {

    @GuardedBy("this")
    private CompletableFuture<Void> closeFuture = null;

    @Override
    public synchronized boolean isClosed() {
        return null != closeFuture;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> savedFuture;
        synchronized (this) {
            if (null != closeFuture) {
                return closeFuture;
            }
            closeFuture = savedFuture = FutureUtils.createFuture();
        }
        closeAsyncOnce(savedFuture);
        return savedFuture;
    }

    /**
     * Do the core logic of closing this instance once.
     *
     * @param closeFuture the future to complete when the instance is closed.
     */
    protected abstract void closeAsyncOnce(CompletableFuture<Void> closeFuture);

}
