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
package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Test;

/**
 * Unit test for {@link LedgerMetadataIndex}.
 */
public class LedgerMetadataIndexTest {

    @Test
    public void testSetExplicitLacDoesNotResurrectDeletedLedger() throws Exception {
        File tmpDir = Files.createTempDirectory("bk-ledger-metadata-index").toFile();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        ByteBuf explicitLac = spy(Unpooled.directBuffer(Long.BYTES * 2));

        try (LedgerMetadataIndex index = new LedgerMetadataIndex(new ServerConfiguration(),
                KeyValueStorageRocksDB.factory, tmpDir.getAbsolutePath(), NullStatsLogger.INSTANCE)) {
            long ledgerId = 1234L;
            index.setMasterKey(ledgerId, new byte[] {'m', 'k'});
            explicitLac.writeLong(ledgerId);
            explicitLac.writeLong(10L);

            CountDownLatch lacCopyStarted = new CountDownLatch(1);
            CountDownLatch allowLacCopy = new CountDownLatch(1);
            CountDownLatch deleteStarted = new CountDownLatch(1);
            CountDownLatch deleteFinished = new CountDownLatch(1);

            doAnswer(invocation -> {
                lacCopyStarted.countDown();
                assertTrue(allowLacCopy.await(10, TimeUnit.SECONDS));
                return invocation.callRealMethod();
            }).when(explicitLac).getBytes(anyInt(), any(byte[].class));

            Future<?> setExplicitLac = executor.submit(() -> {
                index.setExplicitLac(ledgerId, explicitLac);
                return null;
            });

            assertTrue(lacCopyStarted.await(10, TimeUnit.SECONDS));

            Future<?> delete = executor.submit(() -> {
                deleteStarted.countDown();
                try {
                    index.delete(ledgerId);
                } finally {
                    deleteFinished.countDown();
                }
                return null;
            });

            assertTrue(deleteStarted.await(10, TimeUnit.SECONDS));
            deleteFinished.await(1, TimeUnit.SECONDS);
            allowLacCopy.countDown();

            setExplicitLac.get(10, TimeUnit.SECONDS);
            delete.get(10, TimeUnit.SECONDS);

            assertThrows(Bookie.NoLedgerException.class, () -> index.get(ledgerId));
        } finally {
            explicitLac.release();
            executor.shutdownNow();
        }
    }
}
