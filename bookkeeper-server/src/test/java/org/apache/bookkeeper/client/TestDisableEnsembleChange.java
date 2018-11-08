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
package org.apache.bookkeeper.client;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.bookkeeper.util.BookKeeperConstants.FEATURE_DISABLE_ENSEMBLE_CHANGE;
import static org.apache.bookkeeper.util.TestUtils.assertEventuallyTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.util.concurrent.RateLimiter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.feature.SettableFeature;
import org.apache.bookkeeper.feature.SettableFeatureProvider;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Case on Disabling Ensemble Change Feature.
 */
public class TestDisableEnsembleChange extends BookKeeperClusterTestCase {

    private static final Logger logger = LoggerFactory.getLogger(TestDisableEnsembleChange.class);

    public TestDisableEnsembleChange() {
        super(4);
    }

    @Test
    public void testDisableEnsembleChange() throws Exception {
        disableEnsembleChangeTest(true);
    }

    @Test
    public void testDisableEnsembleChangeNotEnoughBookies() throws Exception {
        disableEnsembleChangeTest(false);
    }

    void disableEnsembleChangeTest(boolean startNewBookie) throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(metadataServiceUri)
            .setDelayEnsembleChange(false)
            .setDisableEnsembleChangeFeatureName(FEATURE_DISABLE_ENSEMBLE_CHANGE);

        SettableFeatureProvider featureProvider = new SettableFeatureProvider("test", 0);
        BookKeeper bkc = BookKeeper.forConfig(conf)
                .featureProvider(featureProvider)
                .build();

        SettableFeature disableEnsembleChangeFeature = featureProvider.getFeature(FEATURE_DISABLE_ENSEMBLE_CHANGE);
        disableEnsembleChangeFeature.set(true);

        final byte[] password = new byte[0];
        final LedgerHandle lh = bkc.createLedger(4, 3, 2, BookKeeper.DigestType.CRC32, password);
        final AtomicBoolean finished = new AtomicBoolean(false);
        final AtomicBoolean failTest = new AtomicBoolean(false);
        final byte[] entry = "test-disable-ensemble-change".getBytes(UTF_8);

        assertEquals(1, lh.getLedgerMetadata().getAllEnsembles().size());
        ArrayList<BookieSocketAddress> ensembleBeforeFailure =
                new ArrayList<>(lh.getLedgerMetadata().getAllEnsembles().entrySet().iterator().next().getValue());

        final RateLimiter rateLimiter = RateLimiter.create(10);

        Thread addThread = new Thread() {
            @Override
            public void run() {
                try {
                    while (!finished.get()) {
                        rateLimiter.acquire();
                        lh.addEntry(entry);
                    }
                } catch (Exception e) {
                    logger.error("Exception on adding entry : ", e);
                    failTest.set(true);
                }
            }
        };
        addThread.start();
        Thread.sleep(2000);
        killBookie(0);
        Thread.sleep(2000);
        finished.set(true);
        addThread.join();

        assertFalse("Should not fail adding entries facing one bookie failure when disable ensemble change",
                failTest.get());

        // check the ensemble after failure
        assertEquals("No new ensemble should be added when disable ensemble change.",
                1, lh.getLedgerMetadata().getAllEnsembles().size());
        ArrayList<BookieSocketAddress> ensembleAfterFailure =
                new ArrayList<>(lh.getLedgerMetadata().getAllEnsembles().entrySet().iterator().next().getValue());
        assertArrayEquals(ensembleBeforeFailure.toArray(new BookieSocketAddress[ensembleBeforeFailure.size()]),
                ensembleAfterFailure.toArray(new BookieSocketAddress[ensembleAfterFailure.size()]));

        // enable ensemble change
        disableEnsembleChangeFeature.set(false);
        if (startNewBookie) {
            startNewBookie();
        }

        // reset add thread
        finished.set(false);
        final CountDownLatch failLatch = new CountDownLatch(1);

        addThread = new Thread() {
            @Override
            public void run() {
                try {
                    while (!finished.get()) {
                        lh.addEntry(entry);
                    }
                } catch (Exception e) {
                    logger.error("Exception on adding entry : ", e);
                    failLatch.countDown();
                    failTest.set(true);
                }
            }
        };
        addThread.start();
        failLatch.await(4000, TimeUnit.MILLISECONDS);
        finished.set(true);
        addThread.join();

        if (startNewBookie) {
            assertFalse("Should not fail adding entries when enable ensemble change again.",
                    failTest.get());
            assertFalse("Ledger should be closed when enable ensemble change again.",
                    lh.getLedgerMetadata().isClosed());
            assertEquals("New ensemble should be added when enable ensemble change again.",
                    2, lh.getLedgerMetadata().getAllEnsembles().size());
        } else {
            assertTrue("Should fail adding entries when enable ensemble change again.",
                    failTest.get());
            // The ledger close occurs in the background, so assert that it happens eventually
            assertEventuallyTrue("Ledger should be closed when enable ensemble change again.",
                                 () -> lh.getLedgerMetadata().isClosed());
        }
    }

    @Test
    public void testRetryFailureBookie() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(metadataServiceUri)
            .setDelayEnsembleChange(false)
            .setDisableEnsembleChangeFeatureName(FEATURE_DISABLE_ENSEMBLE_CHANGE);

        SettableFeatureProvider featureProvider = new SettableFeatureProvider("test", 0);
        BookKeeper bkc = BookKeeper.forConfig(conf)
                .featureProvider(featureProvider)
                .build();

        SettableFeature disableEnsembleChangeFeature = featureProvider.getFeature(FEATURE_DISABLE_ENSEMBLE_CHANGE);
        disableEnsembleChangeFeature.set(true);

        LedgerHandle lh = bkc.createLedger(4, 4, 4, BookKeeper.DigestType.CRC32, new byte[] {});
        byte[] entry = "testRetryFailureBookie".getBytes();
        for (int i = 0; i < 10; i++) {
            lh.addEntry(entry);
        }
        // kill a bookie
        ServerConfiguration killedConf = killBookie(0);

        final AtomicInteger res = new AtomicInteger(0xdeadbeef);
        final CountDownLatch addLatch = new CountDownLatch(1);
        AsyncCallback.AddCallback cb = new AsyncCallback.AddCallback() {
            @Override
                public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                    logger.info("Add entry {} completed : rc {}.", entryId, rc);
                    res.set(rc);
                    addLatch.countDown();
                }
        };
        lh.asyncAddEntry(entry, cb, null);
        assertFalse("Add entry operation should not complete.",
                addLatch.await(1000, TimeUnit.MILLISECONDS));
        assertEquals(res.get(), 0xdeadbeef);
        // start the original bookie
        bsConfs.add(killedConf);
        bs.add(startBookie(killedConf));
        assertTrue("Add entry operation should complete at this point.",
                addLatch.await(10000, TimeUnit.MILLISECONDS));
        assertEquals(res.get(), BKException.Code.OK);
    }

    @Test
    public void testRetrySlowBookie() throws Exception {
        final int readTimeout = 2;

        ClientConfiguration conf = new ClientConfiguration();
        conf.setReadEntryTimeout(readTimeout)
            .setAddEntryTimeout(readTimeout)
            .setDelayEnsembleChange(false)
            .setDisableEnsembleChangeFeatureName(FEATURE_DISABLE_ENSEMBLE_CHANGE)
            .setMetadataServiceUri(metadataServiceUri);

        SettableFeatureProvider featureProvider = new SettableFeatureProvider("test", 0);
        BookKeeper bkc = BookKeeper.forConfig(conf)
                .featureProvider(featureProvider)
                .build();

        SettableFeature disableEnsembleChangeFeature = featureProvider.getFeature(FEATURE_DISABLE_ENSEMBLE_CHANGE);
        disableEnsembleChangeFeature.set(true);

        LedgerHandle lh = bkc.createLedger(4, 4, 4, BookKeeper.DigestType.CRC32, new byte[] {});
        byte[] entry = "testRetryFailureBookie".getBytes();
        for (int i = 0; i < 10; i++) {
            lh.addEntry(entry);
        }

        List<BookieSocketAddress> curEns = lh.getCurrentEnsemble();

        final CountDownLatch wakeupLatch = new CountDownLatch(1);
        final CountDownLatch suspendLatch = new CountDownLatch(1);
        sleepBookie(curEns.get(2), wakeupLatch, suspendLatch);

        suspendLatch.await();

        final AtomicInteger res = new AtomicInteger(0xdeadbeef);
        final CountDownLatch addLatch = new CountDownLatch(1);
        AsyncCallback.AddCallback cb = new AsyncCallback.AddCallback() {
            @Override
                public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                    logger.info("Add entry {} completed : rc {}.", entryId, rc);
                    res.set(rc);
                    addLatch.countDown();
                }
        };
        lh.asyncAddEntry(entry, cb, null);
        assertFalse("Add entry operation should not complete.",
                addLatch.await(1000, TimeUnit.MILLISECONDS));
        assertEquals(res.get(), 0xdeadbeef);
        // wait until read timeout
        assertFalse("Add entry operation should not complete even timeout.",
                addLatch.await(readTimeout, TimeUnit.SECONDS));
        assertEquals(res.get(), 0xdeadbeef);
        // wait one more read timeout, to ensure we resend multiple retries
        // to ensure it works correctly
        assertFalse("Add entry operation should not complete even timeout.",
                addLatch.await(readTimeout, TimeUnit.SECONDS));
        assertEquals(res.get(), 0xdeadbeef);
        // wakeup the sleep bookie
        wakeupLatch.countDown();
        assertTrue("Add entry operation should complete at this point.",
                addLatch.await(10000, TimeUnit.MILLISECONDS));
        assertEquals(res.get(), BKException.Code.OK);
    }

}
