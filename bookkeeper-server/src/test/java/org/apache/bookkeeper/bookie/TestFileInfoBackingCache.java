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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.FileInfoBackingCache.CachedFileInfo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for FileInfoBackingCache.
 */
@Slf4j
public class TestFileInfoBackingCache {
    final byte[] masterKey = new byte[0];
    final File baseDir;

    public TestFileInfoBackingCache() throws Exception {
        baseDir = File.createTempFile("foo", "bar");
    }

    @Before
    public void setupDir() throws Exception {
        Assert.assertTrue(baseDir.delete());
        Assert.assertTrue(baseDir.mkdirs());
        baseDir.deleteOnExit();
    }

    @Test
    public void basicTest() throws Exception {
        FileInfoBackingCache cache = new FileInfoBackingCache(
                (ledgerId, createIfNotFound) -> {
                    File f = new File(baseDir, String.valueOf(ledgerId));
                    f.deleteOnExit();
                    return f;
                });
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

        Assert.assertEquals(fi.getRefCount(), 0);
        CachedFileInfo fi4 = cache.loadFileInfo(1, null);
        Assert.assertFalse(fi4 == fi);
        Assert.assertEquals(fi.getRefCount(), 0);
        Assert.assertEquals(fi4.getRefCount(), 1);
        Assert.assertEquals(fi.getLf(), fi4.getLf());
    }

    @Test(expected = IOException.class)
    public void testNoKey() throws Exception {
        FileInfoBackingCache cache = new FileInfoBackingCache(
                (ledgerId, createIfNotFound) -> {
                    Assert.assertFalse(createIfNotFound);
                    throw new Bookie.NoLedgerException(ledgerId);
                });
        cache.loadFileInfo(1, null);
    }

    /**
     * Of course this can't prove they don't exist, but
     * try to shake them out none the less.
     */
    @Test
    public void testForDeadlocks() throws Exception {
        int numThreads = 20;
        int maxLedgerId = 10;
        AtomicBoolean done = new AtomicBoolean(false);
        CountDownLatch success = new CountDownLatch(numThreads);
        FileInfoBackingCache cache = new FileInfoBackingCache(
                (ledgerId, createIfNotFound) -> {
                    File f = new File(baseDir, String.valueOf(ledgerId));
                    f.deleteOnExit();
                    return f;
                });
        for (int i = 0; i < numThreads; i++) {
            Thread t = new Thread(() -> {
                    try {
                        Random r = new Random();
                        List<CachedFileInfo> fileInfos = new ArrayList<>();
                        while (!done.get()) {
                            if (r.nextBoolean() && fileInfos.size() < 5) { // take a reference
                                fileInfos.add(cache.loadFileInfo(r.nextInt(maxLedgerId), masterKey));
                            } else { // release a reference
                                Collections.shuffle(fileInfos);
                                if (!fileInfos.isEmpty()) {
                                    fileInfos.remove(0).release();
                                }
                            }
                        }
                        for (CachedFileInfo fi : fileInfos) {
                            fi.release();
                        }
                        success.countDown();
                    } catch (Exception e) {
                        log.error("Something nasty happened, success will never complete", e);
                    }
                }, "HammerThread-" + i);
            t.setDaemon(true);
            t.start();
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
        done.set(true);
        Assert.assertTrue(success.await(5, TimeUnit.SECONDS));

        // try to load all ledgers again.
        // They should be loaded fresh (i.e. this load should be only reference)
        for (int i = 0; i < maxLedgerId; i++) {
            Assert.assertEquals(1, cache.loadFileInfo(i, masterKey).getRefCount());
        }
    }
}
