/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.bookie.storage.ldb;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The class that manages write cache.
 */
public class WriteCacheManager {
    private static final LinkedBlockingQueue<WriteCache> commonWriteCacheQueue =
            new LinkedBlockingQueue<>();
    private Map<SingleDirectoryDbLedgerStorage, WriteCacheContainer> singleDirectoryWriteCacheMap =
            new ConcurrentHashMap<>();

    public WriteCacheManager(long commonWriteCacheMaxSize, long commonWriteCacheBlockNum, ByteBufAllocator allocator) {
        if (commonWriteCacheBlockNum <= 0 || commonWriteCacheMaxSize <= 0) {
            return;
        }
        long commonWriteCacheBlockSize = commonWriteCacheMaxSize / commonWriteCacheBlockNum;
        for (int i = 0; i < commonWriteCacheMaxSize / commonWriteCacheBlockSize; i++) {
            WriteCache writeCache = new WriteCache(allocator, commonWriteCacheBlockSize);
            writeCache.setCommonWriteCache(true);
            commonWriteCacheQueue.add(writeCache);
        }
    }

    public static LinkedBlockingQueue<WriteCache> getCommonWriteCacheQueue() {
        return commonWriteCacheQueue;
    }

    @VisibleForTesting
    void disableFlushWriteCacheThread() {
        singleDirectoryWriteCacheMap.values().forEach(cache -> {
            cache.disableFlushWriteCacheThread();
        });
    }

    public void addWriteCacheContainer(SingleDirectoryDbLedgerStorage singleDirectoryDbLedgerStorage,
                                       WriteCacheContainer writeCacheContainer) {
        singleDirectoryWriteCacheMap.put(singleDirectoryDbLedgerStorage, writeCacheContainer);
    }

    public long size(SingleDirectoryDbLedgerStorage singleDirectoryDbLedgerStorage) {
        return singleDirectoryWriteCacheMap.get(singleDirectoryDbLedgerStorage).size();
    }


    public long count(SingleDirectoryDbLedgerStorage singleDirectoryDbLedgerStorage) {
        return singleDirectoryWriteCacheMap.get(singleDirectoryDbLedgerStorage).count();
    }


    public WriteCache pollFreeWriteCache(SingleDirectoryDbLedgerStorage singleDirectoryDbLedgerStorage,
                                         long timeOutNanos) throws InterruptedException {
        long absoluteTimeoutNanos = System.nanoTime() + timeOutNanos;
        while (System.nanoTime() < absoluteTimeoutNanos) {
            WriteCache writeCache = singleDirectoryWriteCacheMap
                    .get(singleDirectoryDbLedgerStorage)
                    .pollFreeWriteCache();
            if (writeCache != null) {
                return writeCache;
            }

            writeCache = commonWriteCacheQueue.poll();
            if (writeCache != null) {
                return writeCache;
            }

            // Wait some time and try again
            Thread.sleep(1);
        }
        return null;
    }

    public void flushAsync(SingleDirectoryDbLedgerStorage singleDirectoryDbLedgerStorage, WriteCache cache) {
        singleDirectoryWriteCacheMap.get(singleDirectoryDbLedgerStorage).flushAsync(cache);
    }

    public WriteCache flushAndPollFreeCache(SingleDirectoryDbLedgerStorage singleDirectoryDbLedgerStorage) {
        return singleDirectoryWriteCacheMap.get(singleDirectoryDbLedgerStorage).flushAndPollFreeCache();
    }

    public void flush(SingleDirectoryDbLedgerStorage singleDirectoryDbLedgerStorage) {
        singleDirectoryWriteCacheMap.get(singleDirectoryDbLedgerStorage).flush();
    }

    public boolean hasEntry(SingleDirectoryDbLedgerStorage singleDirectoryDbLedgerStorage,
                            long ledgerId,
                            long entryId) {
        return singleDirectoryWriteCacheMap.get(singleDirectoryDbLedgerStorage).hasEntry(ledgerId, entryId);
    }

    public ByteBuf get(SingleDirectoryDbLedgerStorage singleDirectoryDbLedgerStorage, long ledgerId, long entryId) {
        return singleDirectoryWriteCacheMap.get(singleDirectoryDbLedgerStorage).get(ledgerId, entryId);
    }

    public ByteBuf getLastEntry(SingleDirectoryDbLedgerStorage singleDirectoryDbLedgerStorage, long ledgerId) {
        return singleDirectoryWriteCacheMap.get(singleDirectoryDbLedgerStorage).getLastEntry(ledgerId);
    }

    public void shutdown() throws InterruptedException {
        for (WriteCacheContainer cacheContainer : singleDirectoryWriteCacheMap.values()) {
            cacheContainer.shutdown();
        }
        while (!commonWriteCacheQueue.isEmpty()) {
            WriteCache cache = commonWriteCacheQueue.poll();
            cache.close();
        }
    }
}