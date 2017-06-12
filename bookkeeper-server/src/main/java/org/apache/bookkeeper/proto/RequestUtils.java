/**
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
package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;

/**
 * Utilities for requests.
 */
class RequestUtils {

    public static boolean isFenceRequest(ReadRequest readRequest) {
        return readRequest.hasFlag() && readRequest.getFlag().equals(ReadRequest.Flag.FENCE_LEDGER);
    }

    public static boolean isLongPollReadRequest(ReadRequest readRequest) {
        return !isFenceRequest(readRequest) && readRequest.hasPreviousLAC();
    }

    public static boolean shouldPiggybackEntry(ReadRequest readRequest) {
        return readRequest.hasFlag() && readRequest.getFlag().equals(ReadRequest.Flag.ENTRY_PIGGYBACK);
    }
}
