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

package org.apache.bookkeeper.bookie;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Records the total size, remaining size and the set of ledgers that comprise a entry log.
 */
public class EntryLogMetadata {
    private final long entryLogId;
    private long totalSize;
    private long remainingSize;
    private ConcurrentHashMap<Long, Long> ledgersMap;

    public EntryLogMetadata(long logId) {
        this.entryLogId = logId;

        totalSize = remainingSize = 0;
        ledgersMap = new ConcurrentHashMap<Long, Long>();
    }

    public void addLedgerSize(long ledgerId, long size) {
        totalSize += size;
        remainingSize += size;
        Long ledgerSize = ledgersMap.get(ledgerId);
        if (null == ledgerSize) {
            ledgerSize = 0L;
        }
        ledgerSize += size;
        ledgersMap.put(ledgerId, ledgerSize);
    }

    public void removeLedger(long ledgerId) {
        Long size = ledgersMap.remove(ledgerId);
        if (null == size) {
            return;
        }
        remainingSize -= size;
    }

    public boolean containsLedger(long ledgerId) {
        return ledgersMap.containsKey(ledgerId);
    }

    public double getUsage() {
        if (totalSize == 0L) {
            return 0.0f;
        }
        return (double) remainingSize / totalSize;
    }

    public boolean isEmpty() {
        return ledgersMap.isEmpty();
    }

    public long getEntryLogId() {
        return entryLogId;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public long getRemainingSize() {
        return remainingSize;
    }

    Map<Long, Long> getLedgersMap() {
        return ledgersMap;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ totalSize = ").append(totalSize).append(", remainingSize = ").append(remainingSize)
                .append(", ledgersMap = ").append(ledgersMap).append(" }");
        return sb.toString();
    }

}
