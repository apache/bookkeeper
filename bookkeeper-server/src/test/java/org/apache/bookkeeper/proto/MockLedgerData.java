/*
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
package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.TreeMap;

/**
    Mock ledger data.
 */
public class MockLedgerData {
    final long ledgerId;
    boolean isFenced;
    private TreeMap<Long, ByteBuf> entries = new TreeMap<>();

    MockLedgerData(long ledgerId) {
        this.ledgerId = ledgerId;
    }

    boolean isFenced() {
        return isFenced;
    }

    void fence() {
        isFenced = true;
    }

    void addEntry(long entryId, ByteBuf entry) {
        entries.put(entryId, entry);
    }

    ByteBuf getEntry(long entryId) {
        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            Map.Entry<Long, ByteBuf> lastEntry = entries.lastEntry();
            if (lastEntry != null) {
                return lastEntry.getValue();
            } else {
                return null;
            }
        } else {
            return entries.get(entryId);
        }
    }
}
