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

package org.apache.bookkeeper.clients.impl.internal.api;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.stream.proto.StreamProperties;

/**
 * A meta range client that talks to meta range.
 *
 * <p>It is an internal client to get stream specific metadata.
 */
public interface MetaRangeClient {

    StreamProperties getStreamProps();

    //
    // KeyRange Related Operations
    //

    /**
     * Retrieve the current active {@link org.apache.bookkeeper.stream.protocol.RangeId}s from
     * a particular stream {@code streamId}.
     *
     * @return the current active ranges for a given stream.
     */
    CompletableFuture<HashStreamRanges> getActiveDataRanges();

}
