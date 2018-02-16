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

package org.apache.bookkeeper.bookie;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.FileChannelBackingCache.CachedFileChannel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for FileChannelBackingCache.
 */
@Slf4j
public class TestFileChannelBackingCache {
    final File baseDir;
    final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("backing-cache-test-%d").setDaemon(true).build();
    ExecutorService executor;

    FileChannelBackingCache cache;
    ThreadLocal<Cache<Long, EntryLogger.EntryLogBufferedReadChannel>> logid2ReadChannel;
    Set<CachedFileChannel> allCachedFileChannels = new HashSet<>();

    public TestFileChannelBackingCache() throws Exception {
        baseDir = File.createTempFile("foo", "bar");
    }

    @Before
    public void setup() throws Exception {
        Assert.assertTrue(baseDir.delete());
        Assert.assertTrue(baseDir.mkdirs());
        baseDir.deleteOnExit();
        executor = Executors.newCachedThreadPool(threadFactory);
    }

    @After
    public void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Test
    public void basicTest() throws Exception {
        FileChannelBackingCache cache = new FileChannelBackingCache(this::findFile);
        CachedFileChannel fileChannel = cache.loadFileChannel(1);
        Assert.assertEquals(fileChannel.getRefCount(), 1);
        CachedFileChannel fileChannel2 = cache.loadFileChannel(2);
        Assert.assertEquals(fileChannel2.getRefCount(), 1);
        CachedFileChannel fileChannel3 = cache.loadFileChannel(1);
        Assert.assertEquals(fileChannel, fileChannel3);
        Assert.assertEquals(fileChannel3.getRefCount(), 2);

        // check that it expires correctly
        fileChannel.release();
        fileChannel3.release();

        Assert.assertEquals(fileChannel.getRefCount(), FileChannelBackingCache.DEAD_REF);
        CachedFileChannel fileChannel4 = cache.loadFileChannel(1);
        Assert.assertFalse(fileChannel4 == fileChannel);
        Assert.assertEquals(fileChannel.getRefCount(), FileChannelBackingCache.DEAD_REF);
        Assert.assertEquals(fileChannel4.getRefCount(), 1);
        Assert.assertFalse(fileChannel.fileChannel == fileChannel4.fileChannel);
    }

    @Test
    public void testRefCountRace() throws Exception {
        AtomicBoolean done = new AtomicBoolean(false);
        cache = new FileChannelBackingCache(this::findFile);
        Iterable<Future<Set<CachedFileChannel>>> futures =
                IntStream.range(0, 2).mapToObj(
                        (i) -> {
                            Callable<Set<CachedFileChannel>> c = () -> {
                                Set<CachedFileChannel> allFileChannels = new HashSet<>();
                                while (!done.get()) {
                                    CachedFileChannel fileChannel = cache.loadFileChannel(i);
                                    allFileChannels.add(fileChannel);
                                    fileChannel.release();
                                }
                                return allFileChannels;
                            };
                            return executor.submit(c);
                        }).collect(Collectors.toList());
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
        done.set(true);

        // ensure all threads are finished operating on cache, before checking any
        for (Future<Set<CachedFileChannel>> f : futures) {
            f.get();
        }

        for (Future<Set<CachedFileChannel>> f : futures) {
            for (CachedFileChannel fileChannel : f.get()) {
                Assert.assertEquals(FileChannelBackingCache.DEAD_REF, fileChannel.getRefCount());
            }
        }
    }

    @Test
    public void testRaceBufferedReadChannel() throws Exception {
        AtomicBoolean done = new AtomicBoolean(false);
        cache = new FileChannelBackingCache(this::findFile);
        logid2ReadChannel =
                new ThreadLocal<Cache<Long, EntryLogger.EntryLogBufferedReadChannel>>() {
                    @Override
                    public Cache<Long, EntryLogger.EntryLogBufferedReadChannel> initialValue() {
                        return CacheBuilder.newBuilder().concurrencyLevel(1)
                            .maximumSize(1)
                            //decrease the refCnt
                            .removalListener(removal ->
                                    ((EntryLogger.EntryLogBufferedReadChannel) removal.getValue()).release())
                            .build();
                    }
                };
        Iterable<Future<Set<CachedFileChannel>>> futures =
                LongStream.range(0L, 2L).mapToObj(
                        (i) -> {
                            Callable<Set<CachedFileChannel>> c = () -> {
                                while (!done.get()) {
                                    logid2ReadChannel.get().get(i, () -> getChannelForLogId(i));
                                }
                                return allCachedFileChannels;
                            };
                            return executor.submit(c);
                        }).collect(Collectors.toList());
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
        done.set(true);

        // ensure all threads are finished operating on cache, before checking any
        for (Future<Set<CachedFileChannel>> f : futures) {
            f.get();
        }

        // evict all cachedFileChannel
        logid2ReadChannel.get().invalidateAll();
        for (Future<Set<CachedFileChannel>> f : futures) {
            for (CachedFileChannel cachedFileChannel : f.get()) {
                Assert.assertEquals(FileChannelBackingCache.DEAD_REF, cachedFileChannel.getRefCount());
            }
        }

    }

    private File findFile(long logId) throws IOException{
        File f = new File(baseDir, Long.toHexString(logId) + ".log");
        if (!f.exists()) {
            f.createNewFile();
        }
        f.deleteOnExit();
        return f;
    }

    private EntryLogger.EntryLogBufferedReadChannel getChannelForLogId(long entryLogId) throws IOException {
        EntryLogger.EntryLogBufferedReadChannel brc = logid2ReadChannel.get().getIfPresent(entryLogId);
        if (brc != null) {
            return brc;
        }
        FileChannelBackingCache.CachedFileChannel cachedFileChannel = null;
        try {
            do {
                cachedFileChannel = cache.loadFileChannel(entryLogId);
            } while (!cachedFileChannel.tryRetain());
        } finally {
            if (null != cachedFileChannel) {
                cachedFileChannel.release();
            }
        }
        allCachedFileChannels.add(cachedFileChannel);
        brc = new EntryLogger.EntryLogBufferedReadChannel(cachedFileChannel, 512);
        Cache<Long, EntryLogger.EntryLogBufferedReadChannel> threadCache = logid2ReadChannel.get();
        threadCache.put(entryLogId, brc);
        return brc;
    }

}

