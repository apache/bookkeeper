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

import static java.nio.charset.StandardCharsets.UTF_8

import com.github.dockerjava.api.DockerClient

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import org.apache.bookkeeper.tests.integration.utils.BookKeeperClusterUtils
import org.apache.bookkeeper.tests.integration.utils.MavenClassLoader

import org.jboss.arquillian.junit.Arquillian
import org.jboss.arquillian.test.api.ArquillianResource

import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith

import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(Arquillian.class)
class TestCompatUpgradeYahooCustom {
    private static final Logger LOG = LoggerFactory.getLogger(TestCompatUpgradeYahooCustom.class)
    private static byte[] PASSWD = "foobar".getBytes()

    def yahooRepo = "https://raw.githubusercontent.com/yahoo/bookkeeper/mvn-repo"
    def yahooArtifact = "org.apache.bookkeeper:bookkeeper-server:4.3.1.85-yahoo"

    @ArquillianResource
    DockerClient docker

    int numEntries = 1

    def yahooConfiguredBookKeeper(def classLoader, def zookeeper) {
        // bookkeeper client configured in same way as pulsar configures it
        def bkConf = classLoader.newInstance("org.apache.bookkeeper.conf.ClientConfiguration")

        bkConf.setThrottleValue(0)
        bkConf.setAddEntryTimeout(30)
        bkConf.setReadEntryTimeout(30)
        bkConf.setSpeculativeReadTimeout(0)
        bkConf.setNumChannelsPerBookie(16)
        bkConf.setUseV2WireProtocol(true)
        // we can't specify the class name, because it would try to use the thread
        // context classloader to load, which doesn't have the required classes loaded.
        bkConf.setLedgerManagerType("hierarchical")

        bkConf.enableBookieHealthCheck()
        bkConf.setBookieHealthCheckInterval(60, TimeUnit.SECONDS)
        bkConf.setBookieErrorThresholdPerInterval(5)
        bkConf.setBookieQuarantineTime(1800, TimeUnit.SECONDS)

        bkConf.setZkServers(zookeeper)
        return classLoader.newInstance("org.apache.bookkeeper.client.BookKeeper", bkConf)
    }

    def addEntry(def classLoader, def ledger, def entryData) {
        def promise = new CompletableFuture<Long>()
        def buffer = classLoader.callStaticMethod("io.netty.buffer.Unpooled", "copiedBuffer",
                                                  [entryData.getBytes(UTF_8)])
        def callback = classLoader.createCallback(
            "org.apache.bookkeeper.client.AsyncCallback\$AddCallback",
            { rc, _ledger, entryId, ctx ->
                if (rc != 0) {
                    promise.completeExceptionally(
                        classLoader.callStaticMethod("org.apache.bookkeeper.client.BKException",
                                                     "create", [rc]))
                } else {
                    promise.complete(entryId)
                }
            })
        ledger.asyncAddEntry(buffer, callback, null)
        return promise
    }

    def assertCantWrite(def cl, def ledger) throws Exception {
        try {
            addEntry(cl, ledger, "Shouldn't write").get()
            fail("Shouldn't be able to write to ledger")
        } catch (ExecutionException e) {
            // correct behaviour
            // TODO add more
        }
    }
    def createAndWrite(def cl, def bk) throws Exception {
        def ledger = bk.createLedger(3, 2, cl.digestType("CRC32"), PASSWD)
        LOG.info("Created ledger ${ledger.getId()}")
        for (int i = 0; i < numEntries; i++) {
            addEntry(cl, ledger, "foobar" + i).get()
        }
        return ledger
    }

    def openAndVerifyEntries(def cl, def bk, def ledgerId) throws Exception {
        LOG.info("Opening ledger $ledgerId")
        def ledger = bk.openLedger(ledgerId, cl.digestType("CRC32"), PASSWD)
        def entries = ledger.readEntries(0, ledger.getLastAddConfirmed())
        int j = 0
        while (entries.hasMoreElements()) {
            def e = entries.nextElement()
            Assert.assertEquals(new String(e.getEntry()), "foobar"+ j)
            j++
        }
        ledger.close()
    }

    def List<Long> exerciseClients(List<Long> toVerify) {
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker)
        String currentVersion = BookKeeperClusterUtils.CURRENT_VERSION

        def ledgers = []
        def yahooCL = MavenClassLoader.forArtifact(yahooRepo, yahooArtifact)
        def yahooBK = yahooConfiguredBookKeeper(yahooCL, zookeeper)
        def currentCL = MavenClassLoader.forBookKeeperVersion(currentVersion)
        def currentBK = yahooConfiguredBookKeeper(currentCL, zookeeper)
        try {
            // verify we can read ledgers from previous run
            for (Long id : toVerify) {
                LOG.info("Verifying $id with yahoo client")
                openAndVerifyEntries(yahooCL, yahooBK, id)
                LOG.info("Verifying $id with current client")
                openAndVerifyEntries(currentCL, currentBK, id)
            }
            // yahoo client and create open and read
            def ledger0 = createAndWrite(yahooCL, yahooBK)
            ledgers.add(ledger0.getId())
            ledger0.close()
            openAndVerifyEntries(yahooCL, yahooBK, ledger0.getId())

            // yahoo client can fence on yahoo bookies
            def ledger1 = createAndWrite(yahooCL, yahooBK)
            ledgers.add(ledger1.getId())
            openAndVerifyEntries(yahooCL, yahooBK, ledger1.getId())
            assertCantWrite(yahooCL, ledger1)

            // current client can create open and read
            def ledger2 = createAndWrite(currentCL, currentBK)
            ledgers.add(ledger2.getId())
            ledger2.close()
            openAndVerifyEntries(currentCL, currentBK, ledger2.getId())

            // current client can fence on yahoo bookies
            def ledger3 = createAndWrite(currentCL, currentBK)
            ledgers.add(ledger3.getId())
            openAndVerifyEntries(currentCL, currentBK, ledger3.getId())
            assertCantWrite(currentCL, ledger3)

            // current client can fence a bookie created by yahoo client
            def ledger4 = createAndWrite(yahooCL, yahooBK)
            ledgers.add(ledger4.getId())
            openAndVerifyEntries(currentCL, currentBK, ledger4.getId())
            assertCantWrite(yahooCL, ledger4)

            // Since METADATA_VERSION is upgraded and it is using binary format, the older
            // clients which are expecting text format would fail to read ledger metadata.
            def ledger5 = createAndWrite(currentCL, currentBK)
            ledgers.add(ledger5.getId())
            try {
                openAndVerifyEntries(yahooCL, yahooBK, ledger5.getId())
            } catch (Exception exc) {
                Assert.assertEquals(exc.getClass().getName(),
                  "org.apache.bookkeeper.client.BKException\$ZKException")
            }
        } finally {
            currentBK.close()
            currentCL.close()
            yahooBK.close()
            yahooCL.close()
        }
        return ledgers
    }

    @Test
    public void testUpgradeYahooCustom() throws Exception {
        String currentVersion = BookKeeperClusterUtils.CURRENT_VERSION
        String yahooVersion = "4.3-yahoo"
        BookKeeperClusterUtils.metadataFormatIfNeeded(docker, yahooVersion)

        Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, yahooVersion))
        def preUpgradeLedgers = exerciseClients([])
        Assert.assertTrue(BookKeeperClusterUtils.stopAllBookies(docker))
        BookKeeperClusterUtils.runOnAllBookies(
            docker, "cp", "/opt/bookkeeper/${yahooVersion}/conf/bookkeeper.conf",
            "/opt/bookkeeper/${currentVersion}/conf/bk_server.conf")
        BookKeeperClusterUtils.updateAllBookieConf(docker, currentVersion,
                                                   "logSizeLimit", "1073741824")
        BookKeeperClusterUtils.updateAllBookieConf(docker, currentVersion,
                                                   "statsProviderClass",
                                                   "org.apache.bookkeeper.stats.NullStatsProvider")

        Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, currentVersion))
        // Since METADATA_VERSION is upgraded and it is using binary format, the older
        // clients which are expecting text format would fail to read ledger metadata.
        try {
            exerciseClients(preUpgradeLedgers)
        } catch (Exception exc) {
            Assert.assertEquals(exc.getClass().getName(),
              "org.apache.bookkeeper.client.BKException\$ZKException")
        }
    }
}
