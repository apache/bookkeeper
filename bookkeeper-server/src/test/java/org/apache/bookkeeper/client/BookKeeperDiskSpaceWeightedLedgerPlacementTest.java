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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.TestBookieImpl;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.common.testing.annotations.FlakyTest;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests of the main BookKeeper client.
 */
public class BookKeeperDiskSpaceWeightedLedgerPlacementTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(BookKeeperDiskSpaceWeightedLedgerPlacementTest.class);
    private static final long MS_WEIGHT_UPDATE_TIMEOUT = 30000;

    public BookKeeperDiskSpaceWeightedLedgerPlacementTest() {
        super(10);
    }

    class BookKeeperCheckInfoReader extends BookKeeper {
        BookKeeperCheckInfoReader(ClientConfiguration conf) throws BKException, IOException, InterruptedException {
            super(conf);
        }

        void blockUntilBookieWeightIs(BookieId bookie, Optional<Long> target) throws InterruptedException {
            long startMsecs = System.currentTimeMillis();
            Optional<Long> freeDiskSpace = Optional.empty();
            while (System.currentTimeMillis() < (startMsecs + MS_WEIGHT_UPDATE_TIMEOUT)) {
                freeDiskSpace = bookieInfoReader.getFreeDiskSpace(bookie);
                if (freeDiskSpace.equals(target)) {
                    return;
                }
                Thread.sleep(1000);
            }
            fail(String.format(
                    "Server %s still has weight %s rather than %s",
                    bookie.toString(), freeDiskSpace, target.toString()));
        }
    }

    private BookieServer restartBookie(
            BookKeeperCheckInfoReader client, ServerConfiguration conf, final long initialFreeDiskSpace,
            final long finalFreeDiskSpace, final AtomicBoolean useFinal) throws Exception {
        final AtomicBoolean ready = useFinal == null ? new AtomicBoolean(false) : useFinal;
        Bookie bookieWithCustomFreeDiskSpace = new TestBookieImpl(conf) {
            long startTime = System.currentTimeMillis();
            @Override
            public long getTotalFreeSpace() {
                if (startTime == 0) {
                    startTime = System.currentTimeMillis();
                }
                if (!ready.get()) {
                    return initialFreeDiskSpace;
                } else {
                    // after delaySecs, advertise finalFreeDiskSpace; before that advertise initialFreeDiskSpace
                    return finalFreeDiskSpace;
                }
            }
        };
        BookieServer server = startAndAddBookie(conf, bookieWithCustomFreeDiskSpace).getServer();
        client.blockUntilBookieWeightIs(server.getBookieId(), Optional.of(initialFreeDiskSpace));
        if (useFinal == null) {
            ready.set(true);
        }
        return server;
    }

    private BookieServer replaceBookieWithCustomFreeDiskSpaceBookie(
            BookKeeperCheckInfoReader client,
            int bookieIdx, final long freeDiskSpace)
            throws Exception {
        return replaceBookieWithCustomFreeDiskSpaceBookie(client, bookieIdx, freeDiskSpace, freeDiskSpace, null);
    }

    private BookieServer replaceBookieWithCustomFreeDiskSpaceBookie(
            BookKeeperCheckInfoReader client,
            BookieServer bookie, final long freeDiskSpace)
            throws Exception {
        for (int i = 0; i < bookieCount(); i++) {
            if (addressByIndex(i).equals(bookie.getBookieId())) {
                return replaceBookieWithCustomFreeDiskSpaceBookie(client, i, freeDiskSpace);
            }
        }
        return null;
    }

    private BookieServer replaceBookieWithCustomFreeDiskSpaceBookie(
            BookKeeperCheckInfoReader client,
            int bookieIdx, long initialFreeDiskSpace,
             long finalFreeDiskSpace, AtomicBoolean useFinal) throws Exception {
        BookieId addr = addressByIndex(bookieIdx);
        LOG.info("Killing bookie {}", addr);
        ServerConfiguration conf = killBookieAndWaitForZK(bookieIdx);
        client.blockUntilBookieWeightIs(addr, Optional.empty());
        return restartBookie(client, conf, initialFreeDiskSpace, finalFreeDiskSpace, useFinal);
    }

    /**
     * Test to show that weight based selection honors the disk weight of bookies.
     */
    @FlakyTest("https://github.com/apache/bookkeeper/issues/503")
    public void testDiskSpaceWeightedBookieSelection() throws Exception {
        long freeDiskSpace = 1000000L;
        int multiple = 3;

        ClientConfiguration conf = new ClientConfiguration();
        conf.setDiskWeightBasedPlacementEnabled(true)
            .setGetBookieInfoRetryIntervalSeconds(1, TimeUnit.SECONDS)
            .setBookieMaxWeightMultipleForWeightBasedPlacement(multiple)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        final BookKeeperCheckInfoReader client = new BookKeeperCheckInfoReader(conf);

        for (int i = 0; i < numBookies; i++) {
            // the first 8 bookies have freeDiskSpace of 1MB; While the remaining 2 have 3MB
            if (i < numBookies - 2) {
                replaceBookieWithCustomFreeDiskSpaceBookie(client, 0, freeDiskSpace);
            } else {
                replaceBookieWithCustomFreeDiskSpaceBookie(client, 0, multiple * freeDiskSpace);
            }
        }
        Map<BookieId, Integer> m = new HashMap<>();
        bookieAddresses().forEach(a -> m.put(a, 0));

        for (int i = 0; i < 2000; i++) {
            LedgerHandle lh = client.createLedger(3, 3, DigestType.CRC32, "testPasswd".getBytes());
            for (BookieId b : lh.getLedgerMetadata().getEnsembleAt(0)) {
                m.put(b, m.get(b) + 1);
            }
        }
        client.close();
        // make sure that bookies with higher weight(the last 2 bookies) are chosen 3X as often as the median;
        // since the number of ledgers created is small (2000), we allow a range of 2X to 4X instead of the exact 3X
        for (int i = 0; i < numBookies - 2; i++) {
            double ratio1 = (double) m.get(addressByIndex(numBookies - 2))
                / (double) m.get(addressByIndex(i));
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio1 - multiple),
                    Math.abs(ratio1 - multiple) < 1);
            double ratio2 = (double) m.get(addressByIndex(numBookies - 1))
                / (double) m.get(addressByIndex(i));
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio2 - multiple),
                    Math.abs(ratio2 - multiple) < 1);
        }
    }

    /**
     * Test to show that weight based selection honors the disk weight of bookies and also adapts
     * when the bookies's weight changes.
     */
    @FlakyTest("https://github.com/apache/bookkeeper/issues/503")
    public void testDiskSpaceWeightedBookieSelectionWithChangingWeights() throws Exception {
        long freeDiskSpace = 1000000L;
        int multiple = 3;

        ClientConfiguration conf = new ClientConfiguration();
        conf.setDiskWeightBasedPlacementEnabled(true)
            .setGetBookieInfoRetryIntervalSeconds(1, TimeUnit.SECONDS)
            .setBookieMaxWeightMultipleForWeightBasedPlacement(multiple)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        final BookKeeperCheckInfoReader client = new BookKeeperCheckInfoReader(conf);

        for (int i = 0; i < numBookies; i++) {
            // the first 8 bookies have freeDiskSpace of 1MB; While the remaining 2 have 3MB
            if (i < numBookies - 2) {
                replaceBookieWithCustomFreeDiskSpaceBookie(client, 0, freeDiskSpace);
            } else {
                replaceBookieWithCustomFreeDiskSpaceBookie(client, 0, multiple * freeDiskSpace);
            }
        }
        Map<BookieId, Integer> m = new HashMap<>();
        bookieAddresses().forEach(a -> m.put(a, 0));

        for (int i = 0; i < 2000; i++) {
            LedgerHandle lh = client.createLedger(3, 3, DigestType.CRC32, "testPasswd".getBytes());
            for (BookieId b : lh.getLedgerMetadata().getEnsembleAt(0)) {
                m.put(b, m.get(b) + 1);
            }
        }

        // make sure that bookies with higher weight(the last 2 bookies) are chosen 3X as often as the median;
        // since the number of ledgers created is small (2000), we allow a range of 2X to 4X instead of the exact 3X
        for (int i = 0; i < numBookies - 2; i++) {
            double ratio1 = (double) m.get(addressByIndex(numBookies - 2))
                / (double) m.get(addressByIndex(i));
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio1 - multiple),
                    Math.abs(ratio1 - multiple) < 1);
            double ratio2 = (double) m.get(addressByIndex(numBookies - 1))
                / (double) m.get(addressByIndex(i));
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio2 - multiple),
            Math.abs(ratio2 - multiple) < 1);
        }

        // Restart the bookies in such a way that the first 2 bookies go from 1MB to 3MB free space and the last
        // 2 bookies go from 3MB to 1MB
        BookieServer server1 = serverByIndex(0);
        BookieServer server2 = serverByIndex(1);
        BookieServer server3 = serverByIndex(numBookies - 2);
        BookieServer server4 = serverByIndex(numBookies - 1);

        server1 = replaceBookieWithCustomFreeDiskSpaceBookie(client, server1, multiple * freeDiskSpace);
        server2 = replaceBookieWithCustomFreeDiskSpaceBookie(client, server2, multiple * freeDiskSpace);
        server3 = replaceBookieWithCustomFreeDiskSpaceBookie(client, server3, freeDiskSpace);
        server4 = replaceBookieWithCustomFreeDiskSpaceBookie(client, server4, freeDiskSpace);

        bookieAddresses().forEach(a -> m.put(a, 0));

        for (int i = 0; i < 2000; i++) {
            LedgerHandle lh = client.createLedger(3, 3, DigestType.CRC32, "testPasswd".getBytes());
            for (BookieId b : lh.getLedgerMetadata().getEnsembleAt(0)) {
                m.put(b, m.get(b) + 1);
            }
        }

        // make sure that bookies with higher weight(the last 2 bookies) are chosen 3X as often as the median;
        // since the number of ledgers created is small (2000), we allow a range of 2X to 4X instead of the exact 3X
        for (int i = 0; i < numBookies; i++) {
            if (server1.getLocalAddress().equals(addressByIndex(i))
                    || server2.getLocalAddress().equals(addressByIndex(i))) {
                continue;
            }
            double ratio1 = (double) m.get(server1)
                / (double) m.get(addressByIndex(i));
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio1 - multiple),
                    Math.abs(ratio1 - multiple) < 1);
            double ratio2 = (double) m.get(server2)
                / (double) m.get(addressByIndex(i));
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio2 - multiple),
            Math.abs(ratio2 - multiple) < 1);
        }
        client.close();
    }

    /**
     * Test to show that weight based selection honors the disk weight of bookies and also adapts
     * when bookies go away permanently.
     */
    @FlakyTest("https://github.com/apache/bookkeeper/issues/503")
    public void testDiskSpaceWeightedBookieSelectionWithBookiesDying() throws Exception {
        long freeDiskSpace = 1000000L;
        int multiple = 3;

        ClientConfiguration conf = new ClientConfiguration();
        conf.setDiskWeightBasedPlacementEnabled(true)
            .setGetBookieInfoRetryIntervalSeconds(1, TimeUnit.SECONDS)
            .setBookieMaxWeightMultipleForWeightBasedPlacement(multiple)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        final BookKeeperCheckInfoReader client = new BookKeeperCheckInfoReader(conf);

        for (int i = 0; i < numBookies; i++) {
            // the first 8 bookies have freeDiskSpace of 1MB; While the remaining 2 have 1GB
            if (i < numBookies - 2) {
                replaceBookieWithCustomFreeDiskSpaceBookie(client, 0, freeDiskSpace);
            } else {
                replaceBookieWithCustomFreeDiskSpaceBookie(client, 0, multiple * freeDiskSpace);
            }
        }
        Map<BookieId, Integer> m = new HashMap<>();
        bookieAddresses().forEach(a -> m.put(a, 0));

        for (int i = 0; i < 2000; i++) {
            LedgerHandle lh = client.createLedger(3, 3, DigestType.CRC32, "testPasswd".getBytes());
            for (BookieId b : lh.getLedgerMetadata().getEnsembleAt(0)) {
                m.put(b, m.get(b) + 1);
            }
        }

        // make sure that bookies with higher weight are chosen 3X as often as the median;
        // since the number of ledgers is small (2000), there may be variation
        double ratio1 = (double) m.get(addressByIndex(numBookies - 2))
            / (double) m.get(addressByIndex(0));
        assertTrue("Weigheted placement is not honored: " + Math.abs(ratio1 - multiple),
                Math.abs(ratio1 - multiple) < 1);
        double ratio2 = (double) m.get(addressByIndex(numBookies - 1))
            / (double) m.get(addressByIndex(1));
        assertTrue("Weigheted placement is not honored: " + Math.abs(ratio2 - multiple),
        Math.abs(ratio2 - multiple) < 1);

        // Bring down the 2 bookies that had higher weight; after this the allocation to all
        // the remaining bookies should be uniform
        bookieAddresses().forEach(a -> m.put(a, 0));

        BookieServer server1 = serverByIndex(numBookies - 2);
        BookieServer server2 = serverByIndex(numBookies - 1);
        killBookieAndWaitForZK(numBookies - 1);
        killBookieAndWaitForZK(numBookies - 2);

        for (int i = 0; i < 2000; i++) {
            LedgerHandle lh = client.createLedger(3, 3, DigestType.CRC32, "testPasswd".getBytes());
            for (BookieId b : lh.getLedgerMetadata().getEnsembleAt(0)) {
                m.put(b, m.get(b) + 1);
            }
        }

        // make sure that bookies with higher weight are chosen 3X as often as the median;
        for (int i = 0; i < numBookies - 3; i++) {
            double delta = Math.abs((double) m.get(addressByIndex(i))
                    - (double) m.get(addressByIndex(i + 1)));
            delta = (delta * 100) / (double) m.get(addressByIndex(i + 1));
            // the deviation should be less than 30%
            assertTrue("Weigheted placement is not honored: " + delta, delta <= 30);
        }
        // since the following 2 bookies were down, they shouldn't ever be selected
        assertTrue("Weigheted placement is not honored" + m.get(server1),
                m.get(server1) == 0);
        assertTrue("Weigheted placement is not honored" + m.get(server2),
                m.get(server2) == 0);

        client.close();
    }

    /**
     * Test to show that weight based selection honors the disk weight of bookies and also adapts
     * when bookies are added.
     */
    @FlakyTest("https://github.com/apache/bookkeeper/issues/503")
    public void testDiskSpaceWeightedBookieSelectionWithBookiesBeingAdded() throws Exception {
        long freeDiskSpace = 1000000L;
        int multiple = 3;

        ClientConfiguration conf = new ClientConfiguration();
        conf.setDiskWeightBasedPlacementEnabled(true)
            .setGetBookieInfoRetryIntervalSeconds(1, TimeUnit.SECONDS)
            .setBookieMaxWeightMultipleForWeightBasedPlacement(multiple)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        final BookKeeperCheckInfoReader client = new BookKeeperCheckInfoReader(conf);

        for (int i = 0; i < numBookies; i++) {
            // all the bookies have freeDiskSpace of 1MB
            replaceBookieWithCustomFreeDiskSpaceBookie(client, 0, freeDiskSpace);
        }
        // let the last two bookies be down initially
        ServerConfiguration conf1 = killBookieAndWaitForZK(numBookies - 1);
        ServerConfiguration conf2 = killBookieAndWaitForZK(numBookies - 2);
        Map<BookieId, Integer> m = new HashMap<>();

        bookieAddresses().forEach(a -> m.put(a, 0));

        for (int i = 0; i < 2000; i++) {
            LedgerHandle lh = client.createLedger(3, 3, DigestType.CRC32, "testPasswd".getBytes());
            for (BookieId b : lh.getLedgerMetadata().getEnsembleAt(0)) {
                m.put(b, m.get(b) + 1);
            }
        }

        // make sure that bookies with higher weight are chosen 3X as often as the median;
        // since the number of ledgers is small (2000), there may be variation
        for (int i = 0; i < numBookies - 3; i++) {
            double delta = Math.abs((double) m.get(addressByIndex(i))
                    - (double) m.get(addressByIndex(i + 1)));
            delta = (delta * 100) / (double) m.get(addressByIndex(i + 1));
            // the deviation should be less than 30%
            assertTrue("Weigheted placement is not honored: " + delta, delta <= 30);
        }

        // bring up the two dead bookies; they'll also have 3X more free space than the rest of the bookies
        restartBookie(client, conf1, multiple * freeDiskSpace, multiple * freeDiskSpace, null);
        restartBookie(client, conf2, multiple * freeDiskSpace, multiple * freeDiskSpace, null);

        bookieAddresses().forEach(a -> m.put(a, 0));

        for (int i = 0; i < 2000; i++) {
            LedgerHandle lh = client.createLedger(3, 3, DigestType.CRC32, "testPasswd".getBytes());
            for (BookieId b : lh.getLedgerMetadata().getEnsembleAt(0)) {
                m.put(b, m.get(b) + 1);
            }
        }

        // make sure that bookies with higher weight(the last 2 bookies) are chosen 3X as often as the median;
        // since the number of ledgers created is small (2000), we allow a range of 2X to 4X instead of the exact 3X
        for (int i = 0; i < numBookies - 2; i++) {
            double ratio1 = (double) m.get(addressByIndex(numBookies - 2))
                / (double) m.get(addressByIndex(i));
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio1 - multiple),
                    Math.abs(ratio1 - multiple) < 1);
            double ratio2 = (double) m.get(addressByIndex(numBookies - 1))
                / (double) m.get(addressByIndex(i));
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio2 - multiple),
                    Math.abs(ratio2 - multiple) < 1);
        }
        client.close();
    }

    /**
     * Tests that the bookie selection is based on the amount of free disk space a bookie has. Also make sure that
     * the periodic bookieInfo read is working and causes the new weights to be taken into account.
     */
    @FlakyTest("https://github.com/apache/bookkeeper/issues/503")
    public void testDiskSpaceWeightedBookieSelectionWithPeriodicBookieInfoUpdate() throws Exception {
        long freeDiskSpace = 1000000L;
        int multiple = 3;

        int updateIntervalSecs = 6;
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri())
            .setDiskWeightBasedPlacementEnabled(true)
            .setGetBookieInfoRetryIntervalSeconds(1, TimeUnit.SECONDS)
            .setBookieMaxWeightMultipleForWeightBasedPlacement(multiple)
            .setGetBookieInfoIntervalSeconds(updateIntervalSecs, TimeUnit.SECONDS);
        final BookKeeperCheckInfoReader client = new BookKeeperCheckInfoReader(conf);

        AtomicBoolean useHigherValue = new AtomicBoolean(false);
        for (int i = 0; i < numBookies; i++) {
            // the first 8 bookies have freeDiskSpace of 1MB; the remaining 2 will advertise 1MB for
            // the start of the test, and 3MB once useHigherValue is set
            if (i < numBookies - 2) {
                replaceBookieWithCustomFreeDiskSpaceBookie(client, 0, freeDiskSpace);
            } else {
                replaceBookieWithCustomFreeDiskSpaceBookie(
                        client, 0, freeDiskSpace, multiple * freeDiskSpace, useHigherValue);
            }
        }
        Map<BookieId, Integer> m = new HashMap<>();

        bookieAddresses().forEach(a -> m.put(a, 0));

        for (int i = 0; i < 2000; i++) {
            LedgerHandle lh = client.createLedger(3, 3, DigestType.CRC32, "testPasswd".getBytes());
            for (BookieId b : lh.getLedgerMetadata().getEnsembleAt(0)) {
                m.put(b, m.get(b) + 1);
            }
        }

        for (int i = 0; i < numBookies - 1; i++) {
            double delta = Math.abs((double) m.get(addressByIndex(i))
                    - (double) m.get(addressByIndex(i + 1)));
            delta = (delta * 100) / (double) m.get(addressByIndex(i + 1));
            assertTrue("Weigheted placement is not honored: " + delta, delta <= 30); // the deviation should be <30%
        }


        // Sleep for double the time required to update the bookie infos, and then check each one
        useHigherValue.set(true);
        Thread.sleep(updateIntervalSecs * 1000);
        for (int i = 0; i < numBookies; i++) {
            if (i < numBookies - 2) {
                client.blockUntilBookieWeightIs(addressByIndex(i), Optional.of(freeDiskSpace));
            } else {
                client.blockUntilBookieWeightIs(addressByIndex(i), Optional.of(freeDiskSpace * multiple));
            }
        }

        bookieAddresses().forEach(a -> m.put(a, 0));
        for (int i = 0; i < 2000; i++) {
            LedgerHandle lh = client.createLedger(3, 3, DigestType.CRC32, "testPasswd".getBytes());
            for (BookieId b : lh.getLedgerMetadata().getEnsembleAt(0)) {
                m.put(b, m.get(b) + 1);
            }
        }

        // make sure that bookies with higher weight(the last 2 bookies) are chosen 3X as often as the median;
        // since the number of ledgers created is small (2000), we allow a range of 2X to 4X instead of the exact 3X
        for (int i = 0; i < numBookies - 2; i++) {
            double ratio1 = (double) m.get(addressByIndex(numBookies - 2))
                / (double) m.get(addressByIndex(i));
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio1 - multiple),
                    Math.abs(ratio1 - multiple) < 1);
            double ratio2 = (double) m.get(addressByIndex(lastBookieIndex()))
                / (double) m.get(addressByIndex(i));
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio2 - multiple),
                    Math.abs(ratio2 - multiple) < 1);
        }

        client.close();
    }
}
