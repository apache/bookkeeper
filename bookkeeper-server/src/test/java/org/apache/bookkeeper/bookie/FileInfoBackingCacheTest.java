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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
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
import org.apache.bookkeeper.bookie.FileInfoBackingCache.CachedFileInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for FileInfoBackingCache.
 */
@Slf4j
public class FileInfoBackingCacheTest {
    final byte[] masterKey = new byte[0];
    final File baseDir;
    final ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("backing-cache-test-%d").setDaemon(true).build();
    ExecutorService executor;

    public FileInfoBackingCacheTest() throws Exception {
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

    @Test(timeout = 30000)
    public void basicTest() throws Exception {
        FileInfoBackingCache cache = new FileInfoBackingCache(
                (ledgerId, createIfNotFound) -> {
                    File f = new File(baseDir, String.valueOf(ledgerId));
                    f.deleteOnExit();
                    return f;
                }, FileInfo.CURRENT_HEADER_VERSION);
        CachedFileInfo fi = cache.loadFileInfo(1, masterKey);
        Assert.assertEquals(fi.getRefCount(), 1);
        CachedFileInfo fi2 = cache.loadFileInfo(2, masterKey);
        Assert.assertEquals(fi2.getRefCount(), 1);
        CachedFileInfo fi3 = cache.loadFileInfo(1, null);
        Assert.assertEquals(fi, fi3);
        Assert.assertEquals(fi3.getRefCount(), 2);

        // check that it expires correctly
        fi.release();
        fi3.release();

        Assert.assertEquals(fi.getRefCount(), FileInfoBackingCache.DEAD_REF);
        CachedFileInfo fi4 = cache.loadFileInfo(1, null);
        Assert.assertFalse(fi4 == fi);
        Assert.assertEquals(fi.getRefCount(), FileInfoBackingCache.DEAD_REF);
        Assert.assertEquals(fi4.getRefCount(), 1);
        Assert.assertEquals(fi.getLf(), fi4.getLf());
    }

    @Test(expected = IOException.class, timeout = 30000)
    public void testNoKey() throws Exception {
        FileInfoBackingCache cache = new FileInfoBackingCache(
                (ledgerId, createIfNotFound) -> {
                    Assert.assertFalse(createIfNotFound);
                    throw new Bookie.NoLedgerException(ledgerId);
                }, FileInfo.CURRENT_HEADER_VERSION);
        cache.loadFileInfo(1, null);
    }

    /**
     * Of course this can't prove they don't exist, but
     * try to shake them out none the less.
     */
    @Test(timeout = 30000)
    public void testForDeadlocks() throws Exception {
        int numRunners = 20;
        int maxLedgerId = 10;
        AtomicBoolean done = new AtomicBoolean(false);

        FileInfoBackingCache cache = new FileInfoBackingCache(
                (ledgerId, createIfNotFound) -> {
                    File f = new File(baseDir, String.valueOf(ledgerId));
                    f.deleteOnExit();
                    return f;
                }, FileInfo.CURRENT_HEADER_VERSION);
        Iterable<Future<Set<CachedFileInfo>>> futures =
            IntStream.range(0, numRunners).mapToObj(
                    (i) -> {
                        Callable<Set<CachedFileInfo>> c = () -> {
                            Random r = new Random();
                            List<CachedFileInfo> fileInfos = new ArrayList<>();
                            Set<CachedFileInfo> allFileInfos = new HashSet<>();
                            while (!done.get()) {
                                if (r.nextBoolean() && fileInfos.size() < 5) { // take a reference
                                    CachedFileInfo fi = cache.loadFileInfo(r.nextInt(maxLedgerId), masterKey);
                                    Assert.assertFalse(fi.isClosed());
                                    allFileInfos.add(fi);
                                    fileInfos.add(fi);
                                } else { // release a reference
                                    Collections.shuffle(fileInfos);
                                    if (!fileInfos.isEmpty()) {
                                        fileInfos.remove(0).release();
                                    }
                                }
                            }
                            for (CachedFileInfo fi : fileInfos) {
                                Assert.assertFalse(fi.isClosed());
                                fi.release();
                            }
                            return allFileInfos;
                        };
                        return executor.submit(c);
                    }).collect(Collectors.toList());
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
        done.set(true);

        // ensure all threads are finished operating on cache, before checking any
        for (Future<Set<CachedFileInfo>> f : futures) {
            f.get();
        }

        for (Future<Set<CachedFileInfo>> f : futures) {
            for (CachedFileInfo fi : f.get()) {
                Assert.assertTrue(fi.isClosed());
                Assert.assertEquals(FileInfoBackingCache.DEAD_REF, fi.getRefCount());
            }
        }

        // try to load all ledgers again.
        // They should be loaded fresh (i.e. this load should be only reference)
        for (int i = 0; i < maxLedgerId; i++) {
            Assert.assertEquals(1, cache.loadFileInfo(i, masterKey).getRefCount());
        }
    }

    @Test(timeout = 30000)
    public void testRefCountRace() throws Exception {
        AtomicBoolean done = new AtomicBoolean(false);
        FileInfoBackingCache cache = new FileInfoBackingCache(
                (ledgerId, createIfNotFound) -> {
                    File f = new File(baseDir, String.valueOf(ledgerId));
                    f.deleteOnExit();
                    return f;
                }, FileInfo.CURRENT_HEADER_VERSION);

        Iterable<Future<Set<CachedFileInfo>>> futures =
            IntStream.range(0, 2).mapToObj(
                    (i) -> {
                        Callable<Set<CachedFileInfo>> c = () -> {
                            Set<CachedFileInfo> allFileInfos = new HashSet<>();
                            while (!done.get()) {
                                CachedFileInfo fi = cache.loadFileInfo(1, masterKey);
                                Assert.assertFalse(fi.isClosed());
                                allFileInfos.add(fi);
                                fi.release();
                            }
                            return allFileInfos;
                        };
                        return executor.submit(c);
                    }).collect(Collectors.toList());
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
        done.set(true);

        // ensure all threads are finished operating on cache, before checking any
        for (Future<Set<CachedFileInfo>> f : futures) {
            f.get();
        }

        for (Future<Set<CachedFileInfo>> f : futures) {
            for (CachedFileInfo fi : f.get()) {
                Assert.assertTrue(fi.isClosed());
                Assert.assertEquals(FileInfoBackingCache.DEAD_REF, fi.getRefCount());
            }
        }
    }

    private void guavaEvictionListener(RemovalNotification<Long, CachedFileInfo> notification) {
        notification.getValue().release();
    }

    @Test(timeout = 30000)
    public void testRaceGuavaEvictAndReleaseBeforeRetain() throws Exception {
        AtomicBoolean done = new AtomicBoolean(false);
        Random random = new SecureRandom();
        FileInfoBackingCache cache = new FileInfoBackingCache(
                (ledgerId, createIfNotFound) -> {
                    File f = new File(baseDir, String.valueOf(ledgerId));
                    f.deleteOnExit();
                    return f;
                }, FileInfo.CURRENT_HEADER_VERSION);

        Cache<Long, CachedFileInfo> guavaCache = CacheBuilder.newBuilder()
            .maximumSize(1)
            .removalListener(this::guavaEvictionListener)
            .build();

        Iterable<Future<Set<CachedFileInfo>>> futures =
            LongStream.range(0L, 2L).mapToObj(
                    (i) -> {
                        Callable<Set<CachedFileInfo>> c = () -> {
                            Set<CachedFileInfo> allFileInfos = new HashSet<>();
                            while (!done.get()) {
                                CachedFileInfo fi = null;

                                do {
                                    fi = guavaCache.get(
                                        i, () -> cache.loadFileInfo(i, masterKey));
                                    allFileInfos.add(fi);
                                    Thread.sleep(random.nextInt(100));
                                } while (!fi.tryRetain());

                                Assert.assertFalse(fi.isClosed());
                                fi.release();
                            }
                            return allFileInfos;
                        };
                        return executor.submit(c);
                    }).collect(Collectors.toList());
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
        done.set(true);

        // ensure all threads are finished operating on cache, before checking any
        for (Future<Set<CachedFileInfo>> f : futures) {
            f.get();
        }
        guavaCache.invalidateAll();

        for (Future<Set<CachedFileInfo>> f : futures) {
            for (CachedFileInfo fi : f.get()) {
                Assert.assertTrue(fi.isClosed());
                Assert.assertEquals(FileInfoBackingCache.DEAD_REF, fi.getRefCount());
            }
        }

    }
}
