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
package org.apache.distributedlog.logsegment;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.LogSegmentMetadata;

/**
 * Cache the log segment metadata.
 */
@CustomLog
public class LogSegmentMetadataCache implements RemovalListener<String, LogSegmentMetadata> {

    private final Cache<String, LogSegmentMetadata> cache;
    private final boolean isCacheEnabled;

    public LogSegmentMetadataCache(DistributedLogConfiguration conf,
                                   Ticker ticker) {
        cache = CacheBuilder.newBuilder()
                .concurrencyLevel(conf.getNumWorkerThreads())
                .initialCapacity(1024)
                .expireAfterAccess(conf.getLogSegmentCacheTTLMs(), TimeUnit.MILLISECONDS)
                .maximumSize(conf.getLogSegmentCacheMaxSize())
                .removalListener(this)
                .ticker(ticker)
                .recordStats()
                .build();
        this.isCacheEnabled = conf.isLogSegmentCacheEnabled();
        log.info().attr("cacheEnabled", this.isCacheEnabled).log("Log segment cache configured");
    }

    /**
     * Add the log <i>segment</i> of <i>path</i> to the cache.
     *
     * @param path the path of the log segment
     * @param segment log segment metadata
     */
    public void put(String path, LogSegmentMetadata segment) {
        if (isCacheEnabled) {
            cache.put(path, segment);
        }
    }

    /**
     * Invalid the cache entry associated with <i>path</i>.
     *
     * @param path the path of the log segment
     */
    public void invalidate(String path) {
        if (isCacheEnabled) {
            cache.invalidate(path);
        }
    }

    /**
     * Retrieve the log segment of <i>path</i> from the cache.
     *
     * @param path the path of the log segment.
     * @return log segment metadata if exists, otherwise null.
     */
    public LogSegmentMetadata get(String path) {
        return cache.getIfPresent(path);
    }

    @Override
    public void onRemoval(RemovalNotification<String, LogSegmentMetadata> notification) {
        if (notification.wasEvicted()) {
            log.debug().attr("path", notification.getKey()).log("Log segment was evicted.");
        }
    }
}
