/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.stream.storage.impl.sc;

import io.grpc.Channel;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainer;

/**
 * A storage container that always responds {@link io.grpc.Status#NOT_FOUND}.
 */
final class StorageContainer404 implements StorageContainer {

    static StorageContainer404 of() {
        return INSTANCE;
    }

    private static final StorageContainer404 INSTANCE = new StorageContainer404();

    private StorageContainer404() {}

    @Override
    public long getId() {
        return -404;
    }

    @Override
    public Channel getChannel() {
        return Channel404.of();
    }

    @Override
    public CompletableFuture<StorageContainer> start() {
        return FutureUtils.value(this);
    }

    @Override
    public CompletableFuture<Void> stop() {
        return FutureUtils.Void();
    }

    @Override
    public void close() {
        // no-op
    }
}
