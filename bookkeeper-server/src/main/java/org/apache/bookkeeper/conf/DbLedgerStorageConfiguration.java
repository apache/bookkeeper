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
package org.apache.bookkeeper.conf;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.NettyRuntime;
import io.netty.util.internal.PlatformDependent;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.TimeUnit;

public class DbLedgerStorageConfiguration extends ServerConfiguration {

    private static final int MB = 1024 * 1024;

    @VisibleForTesting
    public static final String WRITE_CACHE_MAX_SIZE_MB = "dbStorage_writeCacheMaxSizeMb";

    private static final long DEFAULT_WRITE_CACHE_MAX_SIZE_MB =
            (long) (0.25 * PlatformDependent.estimateMaxDirectMemory()) / MB;

    @VisibleForTesting
    public static final String READ_AHEAD_CACHE_MAX_SIZE_MB = "dbStorage_readAheadCacheMaxSizeMb";

    private static final long DEFAULT_READ_CACHE_MAX_SIZE_MB =
            (long) (0.25 * PlatformDependent.estimateMaxDirectMemory()) / MB;

    private static final String READ_AHEAD_CACHE_BATCH_SIZE = "dbStorage_readAheadCacheBatchSize";

    private static final int DEFAULT_READ_AHEAD_CACHE_BATCH_SIZE = 100;

    private static final String DIRECT_IO_ENTRYLOGGER = "dbStorage_directIOEntryLogger";

    private static final String DIRECT_IO_ENTRYLOGGER_TOTAL_WRITEBUFFER_SIZE_MB = "dbStorage_directIOEntryLoggerTotalWriteBufferSizeMb";

    private static final long DEFAULT_DIRECT_IO_TOTAL_WRITEBUFFER_SIZE_MB =
            (long) (0.125 * PlatformDependent.estimateMaxDirectMemory()) / MB;

    private static final String DIRECT_IO_ENTRYLOGGER_TOTAL_READBUFFER_SIZE_MB = "dbStorage_directIOEntryLoggerTotalReadBufferSizeMb";

    private static final long DEFAULT_DIRECT_IO_TOTAL_READBUFFER_SIZE_MB =
            (long) (0.125 * PlatformDependent.estimateMaxDirectMemory()) / MB;

    private static final String DIRECT_IO_ENTRYLOGGER_READBUFFER_SIZE_MB = "dbStorage_directIOEntryLoggerReadBufferSizeMb";

    private static final long DEFAULT_DIRECT_IO_READBUFFER_SIZE_MB = 8;

    private static final String DIRECT_IO_ENTRYLOGGER_MAX_FD_CACHE_TIME_SECONDS = "dbStorage_directIOEntryLoggerMaxFdCacheTimeSeconds";

    private static final int DEFAULT_DIRECT_IO_MAX_FD_CACHE_TIME_SECONDS = 300;
    
    @VisibleForTesting
    public static final String MAX_THROTTLE_TIME_MILLIS = "dbStorage_maxThrottleTimeMs";

    private static final long DEFAULT_MAX_THROTTLE_TIME_MILLIS = TimeUnit.SECONDS.toMillis(10);

    public long getWriteCacheMaxSize() {
        return getLongVariableOrDefault(WRITE_CACHE_MAX_SIZE_MB, DEFAULT_WRITE_CACHE_MAX_SIZE_MB) * MB;
    }

    public long getReadCacheMaxSize() {
        return getLongVariableOrDefault(READ_AHEAD_CACHE_MAX_SIZE_MB, DEFAULT_READ_CACHE_MAX_SIZE_MB) * MB;
    }

    public int getReadAheadCacheBatchSize() {
        return this.getInt(READ_AHEAD_CACHE_BATCH_SIZE, DEFAULT_READ_AHEAD_CACHE_BATCH_SIZE);
    }

    public boolean isDirectIOEntryLoggerEnabled() {
        return getBooleanVariableOrDefault(DIRECT_IO_ENTRYLOGGER, false);
    }
    
    public long getDirectIOEntryLoggerTotalWriteBufferSize() {
        return getLongVariableOrDefault(DIRECT_IO_ENTRYLOGGER_TOTAL_WRITEBUFFER_SIZE_MB,
                DEFAULT_DIRECT_IO_TOTAL_WRITEBUFFER_SIZE_MB) * MB;
    }

    public long getDirectIOEntryLoggerTotalReadBufferSize() {
        return getLongVariableOrDefault(DIRECT_IO_ENTRYLOGGER_TOTAL_READBUFFER_SIZE_MB,
                DEFAULT_DIRECT_IO_TOTAL_READBUFFER_SIZE_MB) * MB;
    }

    public long getDirectIOEntryLoggerReadBufferSize() {
        return getLongVariableOrDefault(DIRECT_IO_ENTRYLOGGER_READBUFFER_SIZE_MB, DEFAULT_DIRECT_IO_READBUFFER_SIZE_MB)
                * MB;
    }

    public long getDirectIOEntryLoggerMaxFDCacheTimeSeconds() {
        return getLongVariableOrDefault(DIRECT_IO_ENTRYLOGGER_MAX_FD_CACHE_TIME_SECONDS,
                DEFAULT_DIRECT_IO_MAX_FD_CACHE_TIME_SECONDS);
    }

    public long getMaxThrottleTimeMillis() {
        return this.getLong(MAX_THROTTLE_TIME_MILLIS, DEFAULT_MAX_THROTTLE_TIME_MILLIS);
    }

    /**
     * The configured pooling concurrency for the allocator, if user config it, we should consider the unpooled direct
     * memory which readCache and writeCache occupy when use DbLedgerStorage.
     */
    public int getAllocatorPoolingConcurrency() {
        long writeCacheSize = this.getWriteCacheMaxSize();
        long readCacheSize = this.getReadCacheMaxSize();
        long availableDirectMemory = PlatformDependent.maxDirectMemory() - writeCacheSize - readCacheSize;
        int defaultMinNumArena = NettyRuntime.availableProcessors() * 2;
        final int defaultChunkSize =
                PooledByteBufAllocator.defaultPageSize() << PooledByteBufAllocator.defaultMaxOrder();
        int suitableNum = (int) (availableDirectMemory / defaultChunkSize / 2 / 3);
        return Math.min(defaultMinNumArena, suitableNum);
    }

    private long getLongVariableOrDefault(String keyName, long defaultValue) {
        Object obj = this.getProperty(keyName);
        if (obj instanceof Number) {
            return ((Number) obj).longValue();
        } else if (obj == null) {
            return defaultValue;
        } else if (StringUtils.isEmpty(this.getString(keyName))) {
            return defaultValue;
        } else {
            return this.getLong(keyName);
        }
    }

    private boolean getBooleanVariableOrDefault(String keyName, boolean defaultValue) {
        Object obj = this.getProperty(keyName);
        if (obj instanceof Boolean) {
            return (Boolean) obj;
        } else if (obj == null) {
            return defaultValue;
        } else if (StringUtils.isEmpty(this.getString(keyName))) {
            return defaultValue;
        } else {
            return this.getBoolean(keyName);
        }
    }
}
