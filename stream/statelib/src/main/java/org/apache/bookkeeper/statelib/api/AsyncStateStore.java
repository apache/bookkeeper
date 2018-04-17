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

package org.apache.bookkeeper.statelib.api;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

/**
 * A asynchronous state store that holds the states for stateful applications.
 */
@Public
@Evolving
public interface AsyncStateStore extends AutoCloseable {

    /**
     * Returns the name of the state store.
     *
     * @return the name of the state store.
     */
    String name();

    /**
     * Returns the spec of the state store.
     *
     * @return the spec of the state store.
     */
    StateStoreSpec spec();

    /**
     * Initialize the state store.
     */
    CompletableFuture<Void> init(StateStoreSpec spec);

    /**
     * Close the state store in an asynchronous way.
     */
    CompletableFuture<Void> closeAsync();

    @Override
    default void close() {
        try {
            FutureUtils.result(closeAsync());
        } catch (Exception e) {
            // no-op
        }
    }

}
