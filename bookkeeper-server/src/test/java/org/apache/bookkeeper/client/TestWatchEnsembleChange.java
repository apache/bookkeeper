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
package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.meta.FlatLedgerManagerFactory;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MSLedgerManagerFactory;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.bookkeeper.versioning.Version;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestWatchEnsembleChange extends BookKeeperClusterTestCase {

    static Logger LOG = LoggerFactory.getLogger(TestWatchEnsembleChange.class);

    final DigestType digestType;
    final Class<? extends LedgerManagerFactory> lmFactoryCls;

    public TestWatchEnsembleChange(Class<? extends LedgerManagerFactory> lmFactoryCls) {
        super(7);
        this.digestType = DigestType.CRC32;
        this.lmFactoryCls = lmFactoryCls;
        baseClientConf.setLedgerManagerFactoryClass(lmFactoryCls);
        baseConf.setLedgerManagerFactoryClass(lmFactoryCls);
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
                { FlatLedgerManagerFactory.class },
                { HierarchicalLedgerManagerFactory.class },
                { MSLedgerManagerFactory.class }
        });
    }

    @Test(timeout = 60000)
    public void testWatchEnsembleChange() throws Exception {
        int numEntries = 10;
        LedgerHandle lh = bkc.createLedger(3, 3, 3, digestType, "".getBytes());
        for (int i=0; i<numEntries; i++) {
            lh.addEntry(("data" + i).getBytes());
            LOG.info("Added entry {}.", i);
        }
        LedgerHandle readLh = bkc.openLedgerNoRecovery(lh.getId(), digestType, "".getBytes());
        long lastLAC = readLh.getLastAddConfirmed();
        assertEquals(numEntries - 2, lastLAC);
        ArrayList<BookieSocketAddress> ensemble =
                lh.getLedgerMetadata().currentEnsemble;
        for (BookieSocketAddress addr : ensemble) {
            killBookie(addr);
        }
        // write another batch of entries, which will trigger ensemble change
        for (int i=0; i<numEntries; i++) {
            lh.addEntry(("data" + (numEntries + i)).getBytes());
            LOG.info("Added entry {}.", (numEntries + i));
        }
        TimeUnit.SECONDS.sleep(5);
        readLh.readLastConfirmed();
        assertEquals(2 * numEntries - 2, readLh.getLastAddConfirmed());
        readLh.close();
        lh.close();
    }

    @Test(timeout = 60000)
    public void testWatchMetadataRemoval() throws Exception {
        LedgerManagerFactory factory = ReflectionUtils.newInstance(lmFactoryCls);
        factory.initialize(baseConf, super.zkc, factory.getCurrentVersion());
        final LedgerManager manager = factory.newLedgerManager();
        LedgerIdGenerator idGenerator = factory.newLedgerIdGenerator();

        final ByteBuffer bbLedgerId = ByteBuffer.allocate(8);
        final CountDownLatch createLatch = new CountDownLatch(1);
        final CountDownLatch removeLatch = new CountDownLatch(1);

        idGenerator.generateLedgerId(new GenericCallback<Long>() {
            @Override
            public void operationComplete(int rc, final Long lid) {
                manager.createLedgerMetadata(lid, new LedgerMetadata(4, 2, 2, digestType, "fpj was here".getBytes()),
                         new BookkeeperInternalCallbacks.GenericCallback<Void>(){

                    @Override
                    public void operationComplete(int rc, Void result) {
                        bbLedgerId.putLong(lid);
                        bbLedgerId.flip();
                        createLatch.countDown();
                    }
                });

            }
        });

        assertTrue(createLatch.await(2000, TimeUnit.MILLISECONDS));
        final long createdLid = bbLedgerId.getLong();

        manager.registerLedgerMetadataListener( createdLid,
                new LedgerMetadataListener() {

            @Override
            public void onChanged( long ledgerId, LedgerMetadata metadata ) {
                assertEquals(ledgerId, createdLid);
                assertEquals(metadata, null);
                removeLatch.countDown();
            }
        });

        manager.removeLedgerMetadata( createdLid, Version.ANY,
                new BookkeeperInternalCallbacks.GenericCallback<Void>() {

            @Override
            public void operationComplete(int rc, Void result) {
                assertEquals(rc, BKException.Code.OK);
            }
        });
        assertTrue(removeLatch.await(2000, TimeUnit.MILLISECONDS));
    }
}
