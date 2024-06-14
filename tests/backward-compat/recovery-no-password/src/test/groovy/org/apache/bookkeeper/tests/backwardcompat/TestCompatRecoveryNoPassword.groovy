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
package org.apache.bookkeeper.tests.backwardcompat

import com.github.dockerjava.api.DockerClient

import io.netty.buffer.ByteBuf
import org.apache.bookkeeper.net.BookieId

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import org.apache.bookkeeper.client.BKException
import org.apache.bookkeeper.client.BookKeeper
import org.apache.bookkeeper.client.BookKeeperAdmin
import org.apache.bookkeeper.client.LedgerHandle
import org.apache.bookkeeper.client.api.LedgerMetadata
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.bookkeeper.net.BookieSocketAddress
import org.apache.bookkeeper.proto.BookieProtocol
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback
import org.apache.bookkeeper.tests.integration.utils.BookKeeperClusterUtils
import org.apache.bookkeeper.tests.integration.utils.DockerUtils
import org.apache.bookkeeper.tests.integration.utils.MavenClassLoader

import org.jboss.arquillian.junit.Arquillian
import org.jboss.arquillian.test.api.ArquillianResource

import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith

import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(Arquillian.class)
class TestCompatRecoveryNoPassword {
    private static final Logger LOG = LoggerFactory.getLogger(TestCompatRecoveryNoPassword.class)
    private static byte[] PASSWD = "foobar".getBytes()

    @ArquillianResource
    DockerClient docker

    private LedgerMetadata getLedgerMetadata(BookKeeper bookkeeper, long ledgerId) throws Exception {
        return bookkeeper.getLedgerManager().readLedgerMetadata(ledgerId).get().getValue()
    }

    private static class ReplicationVerificationCallback implements ReadEntryCallback {
        final CountDownLatch latch;
        final AtomicLong numSuccess;

        ReplicationVerificationCallback(int numRequests) {
            latch = new CountDownLatch(numRequests)
            numSuccess = new AtomicLong(0)
        }

        @Override
        public void readEntryComplete(int rc, long ledgerId, long entryId,
                                      ByteBuf buffer, Object ctx) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Got {} for ledger {} entry {} from {}", rc, ledgerId, entryId, ctx)
            }
            if (rc == BKException.Code.OK) {
                numSuccess.incrementAndGet()
            }
            latch.countDown()
        }

        long await() throws InterruptedException {
            if (!latch.await(60, TimeUnit.SECONDS)) {
                LOG.warn("Didn't get all responses in verification");
                return 0;
            } else {
                return numSuccess.get();
            }
        }
    }

    private boolean verifyFullyReplicated(BookKeeper bookkeeper,
                                          LedgerHandle lh,
                                          long untilEntry) throws Exception {
        LedgerMetadata md = getLedgerMetadata(bookkeeper, lh.getId())

        def ensembles = md.getAllEnsembles()

        def ranges = new HashMap<Long, Long>()
        def keyList = new ArrayList(ensembles.keySet())
        Collections.sort(keyList)
        for (int i = 0; i < keyList.size() - 1; i++) {
            ranges.put(keyList.get(i), keyList.get(i + 1))
        }
        ranges.put(keyList.get(keyList.size() - 1), untilEntry)

        for (def e : ensembles.entrySet()) {
            int quorum = md.getAckQuorumSize()
            long startEntryId = e.getKey()
            long endEntryId = ranges.get(startEntryId)
            long expectedSuccess = quorum * (endEntryId - startEntryId)
            int numRequests = e.getValue().size() * ((int) (endEntryId - startEntryId))

            def cb = new ReplicationVerificationCallback(numRequests)
            for (long i = startEntryId; i < endEntryId; i++) {
                for (BookieId addr : e.getValue()) {
                    bookkeeper.getBookieClient()
                        .readEntry(addr, lh.getId(), i, cb, addr, BookieProtocol.FLAG_NONE)
                }
            }

            long numSuccess = cb.await();
            if (numSuccess < expectedSuccess) {
                LOG.warn("Fragment not fully replicated ledgerId = {} startEntryId = {}"
                         + " endEntryId = {} expectedSuccess = {} gotSuccess = {}",
                         lh.getId(), startEntryId, endEntryId, expectedSuccess, numSuccess);
                return false;
            }
        }
        return true;
    }

    /**
     * Test that when we try to recover a ledger which doesn't have
     * the password stored in the configuration, we don't succeed.
     */
    @Test
    public void testRecoveryWithoutPasswordInMetadata() throws Exception {
        int numEntries = 10
        byte[] passwdCorrect = "AAAAAA".getBytes()
        byte[] passwdBad = "BBBBBB".getBytes()

        String currentVersion = BookKeeperClusterUtils.CURRENT_VERSION
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker)

        BookKeeperClusterUtils.legacyMetadataFormat(docker)

        // Create a 4.1.0 client, will update /ledgers/LAYOUT
        def v410CL = MavenClassLoader.forBookKeeperVersion("4.1.0")
        try {
            def v410Conf = v410CL.newInstance("org.apache.bookkeeper.conf.ClientConfiguration")
            v410Conf.setZkServers(zookeeper).setLedgerManagerType("hierarchical")
            def v410BK = v410CL.newInstance("org.apache.bookkeeper.client.BookKeeper", v410Conf)

            // Start bookies
            def bookieContainers = new ArrayList<>(DockerUtils.cubeIdsMatching("bookkeeper"))
            Assert.assertTrue(bookieContainers.size() >= 3)
            Assert.assertTrue(BookKeeperClusterUtils.startBookieWithVersion(
                    docker, bookieContainers.get(0), currentVersion))
            Assert.assertTrue(BookKeeperClusterUtils.startBookieWithVersion(
                    docker, bookieContainers.get(1), currentVersion))

            // recreate bk client so that it reads bookie list
            v410BK.close()
            v410BK = v410CL.newBookKeeper(zookeeper)

            // Write a ledger
            def ledger0 = v410BK.createLedger(2, 2,
                                              v410CL.digestType("MAC"), passwdCorrect)
            for (int i = 0; i < numEntries; i++) {
                ledger0.addEntry("foobar".getBytes())
            }
            ledger0.close()
            v410BK.close()

            // start a new bookie, and kill one of the initial 2
            def failedBookieId = new BookieSocketAddress(
                DockerUtils.getContainerIP(docker, bookieContainers.get(0)), 3181).toBookieId()
            Assert.assertTrue(BookKeeperClusterUtils.stopBookie(
                    docker, bookieContainers.get(0)))
            Assert.assertTrue(BookKeeperClusterUtils.startBookieWithVersion(
                    docker, bookieContainers.get(2), currentVersion))

            def bkCur = new BookKeeper(zookeeper)
            LedgerHandle lh = bkCur.openLedgerNoRecovery(
                ledger0.getId(), BookKeeper.DigestType.MAC, passwdCorrect)
            Assert.assertFalse("Should be entries missing",
                               verifyFullyReplicated(bkCur, lh, numEntries))
            lh.close()

            ClientConfiguration adminConf = new ClientConfiguration()
            adminConf.setZkServers(zookeeper)
            adminConf.setBookieRecoveryDigestType(BookKeeper.DigestType.MAC)
            adminConf.setBookieRecoveryPasswd(passwdBad)

            def bka = new BookKeeperAdmin(adminConf)
            try {
                bka.recoverBookieData(failedBookieId)
                Assert.fail("Shouldn't be able to recover with wrong password")
            } catch (BKException bke) {
                // correct behaviour
            } finally {
                bka.close();
            }

            adminConf.setBookieRecoveryDigestType(BookKeeper.DigestType.CRC32)
            adminConf.setBookieRecoveryPasswd(passwdCorrect)

            bka = new BookKeeperAdmin(adminConf)
            try {
                bka.recoverBookieData(failedBookieId)
                Assert.fail("Shouldn't be able to recover with wrong digest")
            } catch (BKException bke) {
                // correct behaviour
            } finally {
                bka.close();
            }

            // Check that entries are still missing
            lh = bkCur.openLedgerNoRecovery(ledger0.getId(),
                                            BookKeeper.DigestType.MAC, passwdCorrect)
            Assert.assertFalse("Should be entries missing",
                               verifyFullyReplicated(bkCur, lh, numEntries))
            lh.close()


            // Set correct password and mac, recovery will work
            adminConf.setBookieRecoveryDigestType(BookKeeper.DigestType.MAC)
            adminConf.setBookieRecoveryPasswd(passwdCorrect)

            bka = new BookKeeperAdmin(adminConf)
            bka.recoverBookieData(failedBookieId)
            bka.close()

            lh = bkCur.openLedgerNoRecovery(ledger0.getId(),
                                            BookKeeper.DigestType.MAC, passwdCorrect)
            Assert.assertTrue("Should have recovered everything",
                              verifyFullyReplicated(bkCur, lh, numEntries))
            lh.close()
            bkCur.close()
        } finally {
            v410CL.close()
        }
    }
}
