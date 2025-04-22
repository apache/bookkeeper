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

import static org.apache.bookkeeper.tests.integration.utils.BookKeeperClusterUtils.*

import com.github.dockerjava.api.DockerClient
import com.google.common.collect.Lists
import org.apache.bookkeeper.tests.integration.utils.MavenClassLoader
import org.jboss.arquillian.junit.Arquillian
import org.jboss.arquillian.test.api.ArquillianResource
import org.junit.Assert
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
        legacyMetadataFormat(docker)

        LOG.info("Setting ledger storage")

        List<String> versions = Lists.newArrayList(UPGRADE_DOWNGRADE_TEST_VERSIONS)
        versions.add(CURRENT_VERSION)

        File testRocksDbConfDir = new File(getClass().getClassLoader()
                .getResource("TestCompatUpgradeDowngrade/conf/default_rocksdb.conf").toURI()).getParentFile()

        boolean useRocksDbVersion5 = false
        boolean useKxxHash = false
        for (String version: versions) {
            appendToAllBookieConf(docker, version,
                    "ledgerStorageClass",
                    "org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage")

            try {
                // format_version 5 has been supported since RocksDB 6.6.x
                appendToAllBookieConf(docker, version,
                        "dbStorage_rocksDB_format_version", "5")
                // kxxHash has been supported for a very long time
                appendToAllBookieConf(docker, version,
                        "dbStorage_rocksDB_checksum_type", "kxxHash")

                // copy rocksdb.conf to all bookies since some released versions don't contain the format_version
                // which is necessary for backwards compatibility
                copyToAllBookies(docker, version, testRocksDbConfDir)
                appendToAllBookieConf(docker, version,
                        "entryLocationRocksdbConf",
                        "conf/entry_location_rocksdb.conf")
                appendToAllBookieConf(docker, version,
                        "ledgerMetadataRocksdbConf",
                        "conf/ledger_metadata_rocksdb.conf")
                appendToAllBookieConf(docker, version,
                        "defaultRocksdbConf",
                        "conf/default_rocksdb.conf")
            } catch (Exception e) {
                LOG.warn(version + ": Failed to set rocksdb configs, might be ok for some older version", e)
            }
        }
    }

    // will ignore older non-supported versions

    @Test
    public void upgradeDowngrade_for_4_14_x_and_4_15_x() {
        upgradeDowngrade(VERSION_4_14_x, VERSION_4_15_x)
    }

    @Test
    public void upgradeDowngrade_for_4_15_x_and_4_16_x() {
        upgradeDowngrade(VERSION_4_15_x, VERSION_4_16_x)
    }

    @Test
    public void upgradeDowngrade_for_4_16_x_and_4_17_x() {
        upgradeDowngrade(VERSION_4_16_x, VERSION_4_17_x)
    }

    @Test
    public void upgradeDowngrade_for_4_17_x_and_CurrentMaster() {
        String currentVersion = CURRENT_VERSION
        upgradeDowngrade(VERSION_4_17_x, currentVersion)
    }

    private void upgradeDowngrade(String oldVersion, String newVersion) throws Exception {
        LOG.info("Testing upgrade/downgrade to/from from {} to {}", oldVersion, newVersion)

        String zookeeper = zookeeperConnectString(docker)
        int numEntries = 10

        LOG.info("Starting bookies with old version {}", oldVersion)
        Assert.assertTrue(startAllBookiesWithVersion(docker, oldVersion))

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
            Assert.assertTrue(stopAllBookies(docker))
            Assert.assertTrue(startAllBookiesWithVersion(docker, newVersion))

            LOG.info("Reading ledger with old client")
            testRead(ledger0, numEntries, oldBK, oldCL)
            testRead(ledger1, numEntries, oldBK, oldCL)

            LOG.info("Reading ledgers with new client")
            testRead(ledger0, numEntries, newBK, newCL)
            testRead(ledger1, numEntries, newBK, newCL)

            LOG.info("Downgrade: Stopping all bookies, starting with old version {}", oldVersion)
            Assert.assertTrue(stopAllBookies(docker))
            Assert.assertTrue(startAllBookiesWithVersion(docker, oldVersion))

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
            stopAllBookies(docker)

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
