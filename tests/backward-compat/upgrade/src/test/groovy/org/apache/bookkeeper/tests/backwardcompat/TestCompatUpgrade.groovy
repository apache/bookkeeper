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

import org.apache.bookkeeper.tests.integration.utils.BookKeeperClusterUtils
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

@RunWith(Arquillian.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class TestCompatUpgrade {
    private static final Logger LOG = LoggerFactory.getLogger(TestCompatUpgrade.class)
    private static byte[] PASSWD = "foobar".getBytes()

    @ArquillianResource
    DockerClient docker

    private void testUpgrade(String currentlyRunning, String upgradeTo, boolean clientCompatBroken = false,
                             boolean currentlyRunningShutsdownBadly = false) {
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker)
        LOG.info("Upgrading from {} to {}", currentlyRunning, upgradeTo)
        int numEntries = 10
        def currentRunningCL = MavenClassLoader.forBookKeeperVersion(currentlyRunning)
        def currentRunningBK = currentRunningCL.newBookKeeper(zookeeper)
        def upgradedCL = MavenClassLoader.forBookKeeperVersion(upgradeTo)
        def upgradedBK = upgradedCL.newBookKeeper(zookeeper)

        try {
            def ledger0 = currentRunningBK.createLedger(2, 2,
                                                        currentRunningCL.digestType("CRC32"),
                                                        PASSWD)
            for (int i = 0; i < numEntries; i++) {
                ledger0.addEntry(("foobar" + i).getBytes())
            }
            ledger0.close()

            // Check whether current client can write to old server
            def ledger1 = upgradedBK.createLedger(2, 2, upgradedCL.digestType("CRC32"), PASSWD)
            try {
                ledger1.addEntry("foobar".getBytes())

                if (clientCompatBroken) {
                    Assert.fail("Shouldn't have been able to write")
                }
            } catch (Exception e) {
                if (!clientCompatBroken) {
                    throw e;
                }
            }

            if (currentlyRunningShutsdownBadly) {
                // 4.6.0 & 4.6.1 can sometimes leave their ZK session alive
                // eventually the session should timeout though
                for (int i = 0; i < 5; i++) {
                    if (BookKeeperClusterUtils.stopAllBookies(docker)) {
                        break
                    }
                }
                Assert.assertTrue(BookKeeperClusterUtils.stopAllBookies(docker))
            } else {
                Assert.assertTrue(BookKeeperClusterUtils.stopAllBookies(docker))
            }
            Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, upgradeTo))

            // check that old client can read its old ledgers on new server
            def ledger2 = currentRunningBK.openLedger(ledger0.getId(), currentRunningCL.digestType("CRC32"),
                                                      PASSWD)
            Assert.assertEquals(numEntries, ledger2.getLastAddConfirmed() + 1 /* counts from 0 */)
            def entries = ledger2.readEntries(0, ledger2.getLastAddConfirmed())
            int j = 0
            while (entries.hasMoreElements()) {
                def e = entries.nextElement()
                Assert.assertEquals(new String(e.getEntry()), "foobar"+ j)
                j++
            }
            ledger2.close()
        } finally {
            upgradedBK.close()
            upgradedCL.close()
            currentRunningBK.close()
            currentRunningCL.close()
        }
    }

    @Test
    public void test_000() throws Exception {
        BookKeeperClusterUtils.legacyMetadataFormat(docker)
        Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, "4.8.2"))
    }

    @Test
    public void test_001_482to492() throws Exception {
        testUpgrade("4.8.2", "4.9.2")
    }

    @Test
    public void test_002_492to4100() throws Exception {
        testUpgrade("4.9.2", "4.10.0")
    }

    @Test
    public void test_003_4100to4111() throws Exception {
        testUpgrade("4.10.0", "4.11.1")
    }

    @Test
    public void test_004_4111to4121() throws Exception {
        testUpgrade("4.11.1", "4.12.1")
    }

    @Test
    public void test_005_4121to4130() throws Exception {
        testUpgrade("4.12.1", "4.13.0")
    }

    @Test
    public void test_006_4130to4143() throws Exception {
        testUpgrade("4.13.0", "4.14.4")
    }

    @Test
    public void test_007_4143toCurrentMaster() throws Exception {
        testUpgrade("4.14.4", BookKeeperClusterUtils.CURRENT_VERSION)
    }
}
