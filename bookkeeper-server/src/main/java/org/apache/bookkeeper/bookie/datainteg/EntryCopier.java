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

package org.apache.bookkeeper.bookie.datainteg;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.LedgerMetadata;

/**
 * Interface for copying entries from other bookies.
 * The implementation should take care of selecting the order of the replicas
 * from which we try to read, taking into account stickiness and errors.
 * The implementation should take care of rate limiting.
 */
public interface EntryCopier {
    /**
     * Start copying a new batch. In general, there should be a batch per ledger.
     */
    Batch newBatch(long ledgerId, LedgerMetadata metadata) throws IOException;

    /**
     * An interface for a batch to be copied.
     */
    interface Batch {
        /**
         * Copy an entry from a remote bookie and store it locally.
         * @return the number of bytes copied.
         */
        CompletableFuture<Long> copyFromAvailable(long entryId);
    }
}
