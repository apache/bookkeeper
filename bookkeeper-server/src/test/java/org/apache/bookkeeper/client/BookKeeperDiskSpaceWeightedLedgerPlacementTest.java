package org.apache.bookkeeper.client;
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.MathUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * Tests of the main BookKeeper client
 */
public class BookKeeperDiskSpaceWeightedLedgerPlacementTest extends BookKeeperClusterTestCase {
    private final static Logger LOG = LoggerFactory.getLogger(BookKeeperDiskSpaceWeightedLedgerPlacementTest.class);

    public BookKeeperDiskSpaceWeightedLedgerPlacementTest() {
        super(10);
    }
    
    private BookieServer restartBookie(ServerConfiguration conf, final long initialFreeDiskSpace,
            final long finallFreeDiskSpace, final int delaySecs) throws Exception {
        Bookie bookieWithCustomFreeDiskSpace = new Bookie(conf) {
            long startTime = System.currentTimeMillis();
            @Override
            public long getTotalFreeSpace() {
                if (startTime == 0) {
                    startTime = System.currentTimeMillis();
                }
                if (delaySecs == 0 || ((System.currentTimeMillis()) - startTime < delaySecs*1000)) {
                    return initialFreeDiskSpace;
                } else {
                    // after delaySecs, advertise finallFreeDiskSpace; before that advertise initialFreeDiskSpace
                    return finallFreeDiskSpace;
                }
            }
        };
        bsConfs.add(conf);
        BookieServer server = startBookie(conf, bookieWithCustomFreeDiskSpace);
        bs.add(server);
        return server;
    }

    private BookieServer replaceBookieWithCustomFreeDiskSpaceBookie(int bookieIdx, final long freeDiskSpace)
            throws Exception {
        LOG.info("Killing bookie " + bs.get(bookieIdx).getLocalAddress());
        bs.get(bookieIdx).getLocalAddress();
        ServerConfiguration conf = killBookie(bookieIdx);
        return restartBookie(conf, freeDiskSpace, freeDiskSpace, 0);
    }

    private BookieServer replaceBookieWithCustomFreeDiskSpaceBookie(BookieServer bookie, final long freeDiskSpace)
            throws Exception {
        for (int i=0; i < bs.size(); i++) {
            if (bs.get(i).getLocalAddress().equals(bookie.getLocalAddress())) {
                return replaceBookieWithCustomFreeDiskSpaceBookie(i, freeDiskSpace);
            }
        }
        return null;
    }

    private BookieServer replaceBookieWithCustomFreeDiskSpaceBookie(int bookieIdx, long initialFreeDiskSpace,
             long finalFreeDiskSpace, int delay) throws Exception {
        LOG.info("Killing bookie " + bs.get(bookieIdx).getLocalAddress());
        bs.get(bookieIdx).getLocalAddress();
        ServerConfiguration conf = killBookie(bookieIdx);
        return restartBookie(conf, initialFreeDiskSpace, finalFreeDiskSpace, delay);
    }

    /**
     * Test to show that weight based selection honors the disk weight of bookies
     */
    @Test(timeout=60000)
    public void testDiskSpaceWeightedBookieSelection() throws Exception {
        long freeDiskSpace=1000000L;
        int multiple=3;
        for (int i=0; i < numBookies; i++) {
            // the first 8 bookies have freeDiskSpace of 1MB; While the remaining 2 have 3MB
            if (i < numBookies-2) {
                replaceBookieWithCustomFreeDiskSpaceBookie(0, freeDiskSpace);
            } else {
                replaceBookieWithCustomFreeDiskSpaceBookie(0, multiple*freeDiskSpace);
            }
        }
        Map<BookieSocketAddress, Integer> m = new HashMap<BookieSocketAddress, Integer>();
        for (BookieServer b : bs) {
            m.put(b.getLocalAddress(), 0);
        }

        // wait a 100 msecs each for the bookies to come up and the bookieInfo to be retrieved by the client
        ClientConfiguration conf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString()).setDiskWeightBasedPlacementEnabled(true).
            setBookieMaxWeightMultipleForWeightBasedPlacement(multiple);
        Thread.sleep(200);
        final BookKeeper client = new BookKeeper(conf);
        Thread.sleep(200);
        for (int i = 0; i < 2000; i++) {
            LedgerHandle lh = client.createLedger(3, 3, DigestType.CRC32, "testPasswd".getBytes());
            for (BookieSocketAddress b : lh.getLedgerMetadata().getEnsemble(0)) {
                m.put(b, m.get(b)+1);
            }
        }
        client.close();
        // make sure that bookies with higher weight(the last 2 bookies) are chosen 3X as often as the median;
        // since the number of ledgers created is small (2000), we allow a range of 2X to 4X instead of the exact 3X
        for (int i=0; i < numBookies-2; i++) {
            double ratio1 = (double)m.get(bs.get(numBookies-2).getLocalAddress())/(double)m.get(bs.get(i).getLocalAddress());
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio1-multiple), Math.abs(ratio1-multiple) < 1);
            double ratio2 = (double)m.get(bs.get(numBookies-1).getLocalAddress())/(double)m.get(bs.get(i).getLocalAddress());
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio2-multiple), Math.abs(ratio2-multiple) < 1);
        }
    }

    /**
     * Test to show that weight based selection honors the disk weight of bookies and also adapts
     * when the bookies's weight changes.
     */
    @Test(timeout=60000)
    public void testDiskSpaceWeightedBookieSelectionWithChangingWeights() throws Exception {
        long freeDiskSpace=1000000L;
        int multiple=3;
        for (int i=0; i < numBookies; i++) {
            // the first 8 bookies have freeDiskSpace of 1MB; While the remaining 2 have 3MB
            if (i < numBookies-2) {
                replaceBookieWithCustomFreeDiskSpaceBookie(0, freeDiskSpace);
            } else {
                replaceBookieWithCustomFreeDiskSpaceBookie(0, multiple*freeDiskSpace);
            }
        }
        Map<BookieSocketAddress, Integer> m = new HashMap<BookieSocketAddress, Integer>();
        for (BookieServer b : bs) {
            m.put(b.getLocalAddress(), 0);
        }

        // wait a 100 msecs each for the bookies to come up and the bookieInfo to be retrieved by the client
        ClientConfiguration conf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString()).setDiskWeightBasedPlacementEnabled(true).
            setBookieMaxWeightMultipleForWeightBasedPlacement(multiple);
        Thread.sleep(100);
        final BookKeeper client = new BookKeeper(conf);
        Thread.sleep(100);
        for (int i = 0; i < 2000; i++) {
            LedgerHandle lh = client.createLedger(3, 3, DigestType.CRC32, "testPasswd".getBytes());
            for (BookieSocketAddress b : lh.getLedgerMetadata().getEnsemble(0)) {
                m.put(b, m.get(b)+1);
            }
        }

        // make sure that bookies with higher weight(the last 2 bookies) are chosen 3X as often as the median;
        // since the number of ledgers created is small (2000), we allow a range of 2X to 4X instead of the exact 3X
        for (int i=0; i < numBookies-2; i++) {
            double ratio1 = (double)m.get(bs.get(numBookies-2).getLocalAddress())/(double)m.get(bs.get(i).getLocalAddress());
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio1-multiple), Math.abs(ratio1-multiple) < 1);
            double ratio2 = (double)m.get(bs.get(numBookies-1).getLocalAddress())/(double)m.get(bs.get(i).getLocalAddress());
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio2-multiple), Math.abs(ratio2-multiple) < 1);
        }

        // Restart the bookies in such a way that the first 2 bookies go from 1MB to 3MB free space and the last
        // 2 bookies go from 3MB to 1MB
        BookieServer server1 = bs.get(0);
        BookieServer server2 = bs.get(1);
        BookieServer server3 = bs.get(numBookies-2);
        BookieServer server4 = bs.get(numBookies-1);

        server1 = replaceBookieWithCustomFreeDiskSpaceBookie(server1, multiple*freeDiskSpace);
        server2 = replaceBookieWithCustomFreeDiskSpaceBookie(server2, multiple*freeDiskSpace);
        server3 = replaceBookieWithCustomFreeDiskSpaceBookie(server3, freeDiskSpace);
        server4 = replaceBookieWithCustomFreeDiskSpaceBookie(server4, freeDiskSpace);

        Thread.sleep(100);
        for (BookieServer b : bs) {
            m.put(b.getLocalAddress(), 0);
        }
        for (int i = 0; i < 2000; i++) {
            LedgerHandle lh = client.createLedger(3, 3, DigestType.CRC32, "testPasswd".getBytes());
            for (BookieSocketAddress b : lh.getLedgerMetadata().getEnsemble(0)) {
                m.put(b, m.get(b)+1);
            }
        }

        // make sure that bookies with higher weight(the last 2 bookies) are chosen 3X as often as the median;
        // since the number of ledgers created is small (2000), we allow a range of 2X to 4X instead of the exact 3X
        for (int i=0; i < numBookies; i++) {
            if (server1.getLocalAddress().equals(bs.get(i).getLocalAddress()) ||
                server2.getLocalAddress().equals(bs.get(i).getLocalAddress())) {
                continue;
            }
            double ratio1 = (double)m.get(server1.getLocalAddress())/(double)m.get(bs.get(i).getLocalAddress());
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio1-multiple), Math.abs(ratio1-multiple) < 1);
            double ratio2 = (double)m.get(server2.getLocalAddress())/(double)m.get(bs.get(i).getLocalAddress());
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio2-multiple), Math.abs(ratio2-multiple) < 1);
        }
        client.close();
    }

    /**
     * Test to show that weight based selection honors the disk weight of bookies and also adapts
     * when bookies go away permanently.
     */
    @Test(timeout=60000)
    public void testDiskSpaceWeightedBookieSelectionWithBookiesDying() throws Exception {
        long freeDiskSpace=1000000L;
        int multiple=3;
        for (int i=0; i < numBookies; i++) {
            // the first 8 bookies have freeDiskSpace of 1MB; While the remaining 2 have 1GB
            if (i < numBookies-2) {
                replaceBookieWithCustomFreeDiskSpaceBookie(0, freeDiskSpace);
            } else {
                replaceBookieWithCustomFreeDiskSpaceBookie(0, multiple*freeDiskSpace);
            }
        }
        Map<BookieSocketAddress, Integer> m = new HashMap<BookieSocketAddress, Integer>();
        for (BookieServer b : bs) {
            m.put(b.getLocalAddress(), 0);
        }

        // wait a couple of 100 msecs each for the bookies to come up and the bookieInfo to be retrieved by the client
        ClientConfiguration conf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString()).setDiskWeightBasedPlacementEnabled(true).
            setBookieMaxWeightMultipleForWeightBasedPlacement(multiple);
        Thread.sleep(100);
        final BookKeeper client = new BookKeeper(conf);
        Thread.sleep(100);
        for (int i = 0; i < 2000; i++) {
            LedgerHandle lh = client.createLedger(3, 3, DigestType.CRC32, "testPasswd".getBytes());
            for (BookieSocketAddress b : lh.getLedgerMetadata().getEnsemble(0)) {
                m.put(b, m.get(b)+1);
            }
        }

        // make sure that bookies with higher weight are chosen 3X as often as the median;
        // since the number of ledgers is small (2000), there may be variation 
        double ratio1 = (double)m.get(bs.get(numBookies-2).getLocalAddress())/(double)m.get(bs.get(0).getLocalAddress());
        assertTrue("Weigheted placement is not honored: " + Math.abs(ratio1-multiple), Math.abs(ratio1-multiple) < 1);
        double ratio2 = (double)m.get(bs.get(numBookies-1).getLocalAddress())/(double)m.get(bs.get(1).getLocalAddress());
        assertTrue("Weigheted placement is not honored: " + Math.abs(ratio2-multiple), Math.abs(ratio2-multiple) < 1);

        // Bring down the 2 bookies that had higher weight; after this the allocation to all
        // the remaining bookies should be uniform
        for (BookieServer b : bs) {
            m.put(b.getLocalAddress(), 0);
        }
        BookieServer server1 = bs.get(numBookies-2);
        BookieServer server2 = bs.get(numBookies-1);
        killBookie(numBookies-1);
        killBookie(numBookies-2);

        // give some time for the cluster to become stable
        Thread.sleep(100);
        for (int i = 0; i < 2000; i++) {
            LedgerHandle lh = client.createLedger(3, 3, DigestType.CRC32, "testPasswd".getBytes());
            for (BookieSocketAddress b : lh.getLedgerMetadata().getEnsemble(0)) {
                m.put(b, m.get(b)+1);
            }
        }

        // make sure that bookies with higher weight are chosen 3X as often as the median;
        for (int i=0; i < numBookies-3; i++) {
            double delta = Math.abs((double)m.get(bs.get(i).getLocalAddress())-(double)m.get(bs.get(i+1).getLocalAddress()));
            delta = (delta*100)/(double)m.get(bs.get(i+1).getLocalAddress());
            assertTrue("Weigheted placement is not honored: " + delta, delta <= 30); // the deviation should be less than 30%
        }
        // since the following 2 bookies were down, they shouldn't ever be selected
        assertTrue("Weigheted placement is not honored" + m.get(server1.getLocalAddress()),
                m.get(server1.getLocalAddress()) == 0);
        assertTrue("Weigheted placement is not honored" + m.get(server2.getLocalAddress()),
                m.get(server2.getLocalAddress()) == 0);

        client.close();
    }

    /**
     * Test to show that weight based selection honors the disk weight of bookies and also adapts
     * when bookies are added.
     */
    @Test(timeout=60000)
    public void testDiskSpaceWeightedBookieSelectionWithBookiesBeingAdded() throws Exception {
        long freeDiskSpace=1000000L;
        int multiple=3;
        for (int i=0; i < numBookies; i++) {
            // all the bookies have freeDiskSpace of 1MB
            replaceBookieWithCustomFreeDiskSpaceBookie(0, freeDiskSpace);
        }
        // let the last two bookies be down initially
        ServerConfiguration conf1 = killBookie(numBookies-1);
        ServerConfiguration conf2 = killBookie(numBookies-2);
        Map<BookieSocketAddress, Integer> m = new HashMap<BookieSocketAddress, Integer>();
        for (BookieServer b : bs) {
            m.put(b.getLocalAddress(), 0);
        }

        // wait a bit for the bookies to come up and the bookieInfo to be retrieved by the client
        ClientConfiguration conf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString()).setDiskWeightBasedPlacementEnabled(true).
            setBookieMaxWeightMultipleForWeightBasedPlacement(multiple);
        Thread.sleep(100);
        final BookKeeper client = new BookKeeper(conf);
        Thread.sleep(100);
        for (int i = 0; i < 2000; i++) {
            LedgerHandle lh = client.createLedger(3, 3, DigestType.CRC32, "testPasswd".getBytes());
            for (BookieSocketAddress b : lh.getLedgerMetadata().getEnsemble(0)) {
                m.put(b, m.get(b)+1);
            }
        }

        // make sure that bookies with higher weight are chosen 3X as often as the median;
        // since the number of ledgers is small (2000), there may be variation
        for (int i=0; i < numBookies-3; i++) {
            double delta = Math.abs((double)m.get(bs.get(i).getLocalAddress())-(double)m.get(bs.get(i+1).getLocalAddress()));
            delta = (delta*100)/(double)m.get(bs.get(i+1).getLocalAddress());
            assertTrue("Weigheted placement is not honored: " + delta, delta <= 30); // the deviation should be less than 30%
        }

        // bring up the two dead bookies; they'll also have 3X more free space than the rest of the bookies
        restartBookie(conf1, multiple*freeDiskSpace, multiple*freeDiskSpace, 0);
        restartBookie(conf2, multiple*freeDiskSpace, multiple*freeDiskSpace, 0);

        // give some time for the cluster to become stable
        Thread.sleep(100);
        for (BookieServer b : bs) {
            m.put(b.getLocalAddress(), 0);
        }
        for (int i = 0; i < 2000; i++) {
            LedgerHandle lh = client.createLedger(3, 3, DigestType.CRC32, "testPasswd".getBytes());
            for (BookieSocketAddress b : lh.getLedgerMetadata().getEnsemble(0)) {
                m.put(b, m.get(b)+1);
            }
        }

        // make sure that bookies with higher weight(the last 2 bookies) are chosen 3X as often as the median;
        // since the number of ledgers created is small (2000), we allow a range of 2X to 4X instead of the exact 3X
        for (int i=0; i < numBookies-2; i++) {
            double ratio1 = (double)m.get(bs.get(numBookies-2).getLocalAddress())/(double)m.get(bs.get(i).getLocalAddress());
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio1-multiple), Math.abs(ratio1-multiple) < 1);
            double ratio2 = (double)m.get(bs.get(numBookies-1).getLocalAddress())/(double)m.get(bs.get(i).getLocalAddress());
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio2-multiple), Math.abs(ratio2-multiple) < 1);
        }
        client.close();
    }

    /**
     * Tests that the bookie selection is based on the amount of free disk space a bookie has. Also make sure that
     * the periodic bookieInfo read is working and causes the new weights to be taken into account.
     */
    @Test(timeout=60000)
    public void testDiskSpaceWeightedBookieSelectionWithPeriodicBookieInfoUpdate() throws Exception {
        long freeDiskSpace=1000000L;
        int multiple=3;
        for (int i=0; i < numBookies; i++) {
            // the first 8 bookies have freeDiskSpace of 1MB; the remaining 2 will advertise 1MB for
            // the first 3 seconds but then they'll advertise 3MB after the first 3 seconds
            if (i < numBookies-2) {
                replaceBookieWithCustomFreeDiskSpaceBookie(0, freeDiskSpace);
            } else {
                replaceBookieWithCustomFreeDiskSpaceBookie(0, freeDiskSpace, multiple*freeDiskSpace, 2);
            }
        }
        Map<BookieSocketAddress, Integer> m = new HashMap<BookieSocketAddress, Integer>();
        for (BookieServer b : bs) {
            m.put(b.getLocalAddress(), 0);
        }

        // the periodic bookieInfo is read once every 7 seconds
        int updateIntervalSecs = 6;
        ClientConfiguration conf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString()).setDiskWeightBasedPlacementEnabled(true).
            setBookieMaxWeightMultipleForWeightBasedPlacement(multiple).
            setGetBookieInfoIntervalSeconds(updateIntervalSecs, TimeUnit.SECONDS);
        // wait a bit for the bookies to come up and the bookieInfo to be retrieved by the client
        Thread.sleep(100);
        final BookKeeper client = new BookKeeper(conf);
        Thread.sleep(100);
        long startMsecs = MathUtils.now();
        for (int i = 0; i < 2000; i++) {
            LedgerHandle lh = client.createLedger(3, 3, DigestType.CRC32, "testPasswd".getBytes());
            for (BookieSocketAddress b : lh.getLedgerMetadata().getEnsemble(0)) {
                m.put(b, m.get(b)+1);
            }
        }
        long elapsedMsecs = MathUtils.now() - startMsecs;

        // make sure that all the bookies are chosen pretty much uniformly
        int bookiesToCheck = numBookies-1;
        if (elapsedMsecs > updateIntervalSecs*1000) {
            // if this task longer than updateIntervalSecs, the weight for the last 2 bookies will be
            // higher, so skip checking them
            bookiesToCheck = numBookies-3;
        }
        for (int i=0; i < bookiesToCheck; i++) {
            double delta = Math.abs((double)m.get(bs.get(i).getLocalAddress())-(double)m.get(bs.get(i+1).getLocalAddress()));
            delta = (delta*100)/(double)m.get(bs.get(i+1).getLocalAddress());
            assertTrue("Weigheted placement is not honored: " + delta, delta <= 30); // the deviation should be <30%
        }

        if (elapsedMsecs < updateIntervalSecs*1000) {
            // sleep until periodic bookie info retrieval kicks in and it gets the updated
            // freeDiskSpace for the last 2 bookies
            Thread.sleep(updateIntervalSecs*1000 - elapsedMsecs);
        }

        for (BookieServer b : bs) {
            m.put(b.getLocalAddress(), 0);
        }
        for (int i = 0; i < 2000; i++) {
            LedgerHandle lh = client.createLedger(3, 3, DigestType.CRC32, "testPasswd".getBytes());
            for (BookieSocketAddress b : lh.getLedgerMetadata().getEnsemble(0)) {
                m.put(b, m.get(b)+1);
            }
        }

        // make sure that bookies with higher weight(the last 2 bookies) are chosen 3X as often as the median;
        // since the number of ledgers created is small (2000), we allow a range of 2X to 4X instead of the exact 3X
        for (int i=0; i < numBookies-2; i++) {
            double ratio1 = (double)m.get(bs.get(numBookies-2).getLocalAddress())/(double)m.get(bs.get(i).getLocalAddress());
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio1-multiple), Math.abs(ratio1-multiple) < 1);
            double ratio2 = (double)m.get(bs.get(numBookies-1).getLocalAddress())/(double)m.get(bs.get(i).getLocalAddress());
            assertTrue("Weigheted placement is not honored: " + Math.abs(ratio2-multiple), Math.abs(ratio2-multiple) < 1);
        }

        client.close();
    }
}
