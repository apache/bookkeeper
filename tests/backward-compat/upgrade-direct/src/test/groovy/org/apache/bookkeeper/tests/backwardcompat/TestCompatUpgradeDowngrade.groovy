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
import java.util.concurrent.TimeUnit
import org.apache.bookkeeper.tests.integration.utils.BookKeeperClusterUtils
import org.apache.bookkeeper.tests.integration.utils.MavenClassLoader
import org.jboss.arquillian.junit.Arquillian
import org.jboss.arquillian.test.api.ArquillianResource
import org.junit.AfterClass
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.FixMethodOrder
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.MethodSorters
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Sequentially upgrade bookies with different versions and check compatibility.
 * Uses DbLedgerStorage/RocksDB.
 */
@RunWith(Arquillian.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class TestCompatUpgradeDowngrade {
    private static final Logger LOG = LoggerFactory.getLogger(TestCompatUpgradeDowngrade.class)
    private static byte[] PASSWD = "foobar".getBytes()

    private static final String DIGEST_TYPE = "CRC32C"

    static {
        Thread.setDefaultUncaughtExceptionHandler { Thread t, Throwable e ->
            LOG.error("Uncaught exception in thread {}", t, e)
        }
    }

    @ArquillianResource
    DockerClient docker

    @Test
    public void atest_000_setUp() throws Exception {
        LOG.info("Running metaformat")
        BookKeeperClusterUtils.legacyMetadataFormat(docker)

        LOG.info("Setting ledger storage")

        for (String version: BookKeeperClusterUtils.OLD_CLIENT_VERSIONS) {
            BookKeeperClusterUtils.appendToAllBookieConf(docker, version,
                    "ledgerStorageClass",
                    "org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage")
        }
        BookKeeperClusterUtils.appendToAllBookieConf(docker, BookKeeperClusterUtils.CURRENT_VERSION,
                "ledgerStorageClass",
                "org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage")
    }


    // will ignore older non-supported versions

    @Test
    public void upgradeDowngrade_010() {
        upgradeDowngrade("4.12.1", "4.13.0")
    }

    @Test
    public void upgradeDowngrade_011() {
        upgradeDowngrade("4.13.0", "4.14.8")
    }

    @Test
    public void upgradeDowngrade_012() {
        upgradeDowngrade("4.14.8", "4.15.5")
    }

    @Test
    public void upgradeDowngrade_013() {
        upgradeDowngrade("4.15.5", "4.16.5")
    }

    @Test
    public void upgradeDowngrade_014() {
        upgradeDowngrade("4.16.5", "4.17.0")
    }

    @Test
    public void upgradeDowngrade_015() {
        String currentVersion = BookKeeperClusterUtils.CURRENT_VERSION
        BookKeeperClusterUtils.appendToAllBookieConf(docker, currentVersion,
                "dbStorage_rocksDB_format_version",
                "2")
        BookKeeperClusterUtils.appendToAllBookieConf(docker, currentVersion,
                "dbStorage_rocksDB_checksum_type",
                "kCRC32c")
        BookKeeperClusterUtils.appendToAllBookieConf(docker, currentVersion,
                "conf/default_rocksdb.conf.default",
                "format_version",
                "2")
        upgradeDowngrade("4.17.0", currentVersion)
    }

    private void upgradeDowngrade(String oldVersion, String newVersion) throws Exception {
        LOG.info("Testing upgrade/downgrade to/from from {} to {}", oldVersion, newVersion)

        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker)
        int numEntries = 10

        LOG.info("Starting bookies with old version {}", oldVersion)
        Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, oldVersion))

        def oldCL = MavenClassLoader.forBookKeeperVersion(oldVersion)
        def oldBK = oldCL.newBookKeeper(zookeeper)
        def newCL = MavenClassLoader.forBookKeeperVersion(newVersion)
        def newBK = newCL.newBookKeeper(zookeeper)
        try {
            LOG.info("Writing ledgers with old client, reading with new")
            long ledger0 = writeLedger(numEntries, oldBK, oldCL)
            testRead(ledger0, numEntries, newBK, newCL)

            LOG.info("Writing ledgers with new client, reading with old")
            long ledger1 = writeLedger(numEntries, newBK, newCL)
            testRead(ledger1, numEntries, oldBK, oldCL)

            LOG.info("Upgrade: Stopping all bookies, starting with new version {}", newVersion)
            Assert.assertTrue(BookKeeperClusterUtils.stopAllBookies(docker))
            Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, newVersion))

            LOG.info("Reading ledger with old client")
            testRead(ledger0, numEntries, oldBK, oldCL)
            testRead(ledger1, numEntries, oldBK, oldCL)

            LOG.info("Reading ledgers with new client")
            testRead(ledger0, numEntries, newBK, newCL)
            testRead(ledger1, numEntries, newBK, newCL)

            LOG.info("Downgrade: Stopping all bookies, starting with old version {}", oldVersion)
            Assert.assertTrue(BookKeeperClusterUtils.stopAllBookies(docker))
            Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, oldVersion))

            LOG.info("Reading ledgers with old client")
            testRead(ledger0, numEntries, oldBK, oldCL)
            testRead(ledger1, numEntries, oldBK, oldCL)

            LOG.info("Reading ledgers with new client")
            testRead(ledger0, numEntries, newBK, newCL)
            testRead(ledger1, numEntries, newBK, newCL)

            LOG.info("Writing ledgers with old client, reading with new")
            long ledger3 = writeLedger(numEntries, oldBK, oldCL)
            testRead(ledger3, numEntries, newBK, newCL)

            LOG.info("Writing ledgers with new client, reading with old")
            long ledger4 = writeLedger(numEntries, newBK, newCL)
            testRead(ledger4, numEntries, newBK, newCL)

        } finally {
            BookKeeperClusterUtils.stopAllBookies(docker)

            newBK.close()
            newCL.close()
            oldBK.close()
            oldCL.close()
        }
    }

    private long writeLedger(int numEntries, Object client, Object classLoader) {
        def ledger = client.createLedger(3, 2,
                                         classLoader.digestType(DIGEST_TYPE),
                                         PASSWD)
        long ledgerId = ledger.getId()
        try {
            for (int i = 0; i < numEntries; i++) {
                ledger.addEntry(("foobar" + i).getBytes())
            }
        } finally {
            ledger.close()
        }

        return ledgerId
    }

    private void testRead(long ledgerId, int numEntries, Object client, Object classLoader) {
        def ledger = client.openLedger(ledgerId, classLoader.digestType(DIGEST_TYPE), PASSWD)
        try {
            Assert.assertEquals(numEntries, ledger.getLastAddConfirmed() + 1 /* counts from 0 */)
            def entries = ledger.readEntries(0, ledger.getLastAddConfirmed())
            int j = 0
            while (entries.hasMoreElements()) {
                def e = entries.nextElement()
                Assert.assertEquals(new String(e.getEntry()), "foobar"+ j)
                j++
            }
        } finally {
            ledger.close()
        }
    }

}
