/**
 *
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
 *
 */
package org.apache.bookkeeper.client.api;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Unstable;

/**
 * Provide the ability to enforce durability guarantees to the writer.
 *
 * @see WriteAdvHandle
 * @see WriteHandle
 *
 * @since 4.8
 */
@Public
@Unstable
public interface ForceableHandle {

    /**
     * Enforce durability to the entries written by this handle.
     * <p>This API is useful with {@link WriteFlag#DEFERRED_SYNC}, because with
     * that flag writes are acknowledged by the bookie without waiting for a
     * durable write
     * </p>
     *
     * @return an handle to the result
     */
    CompletableFuture<Void> force();

}
