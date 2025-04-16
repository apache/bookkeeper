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

import static org.apache.bookkeeper.tests.integration.utils.BookKeeperClusterUtils.VERSION_4_1_x

import com.github.dockerjava.api.DockerClient
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
class TestCompatUpgradeDirect {
    private static final Logger LOG = LoggerFactory.getLogger(TestCompatUpgradeDirect.class)
    private static byte[] PASSWD = "foobar".getBytes()

    static {
        Thread.setDefaultUncaughtExceptionHandler { Thread t, Throwable e ->
            LOG.error("Uncaught exception in thread {}", t, e)
        }
    }

    @ArquillianResource
    DockerClient docker

    @Test
    public void test0_upgradeDirect410toCurrent() throws Exception {
        BookKeeperClusterUtils.legacyMetadataFormat(docker)
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker)
        String currentVersion = BookKeeperClusterUtils.CURRENT_VERSION
        int numEntries = 10

        Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, VERSION_4_1_x))
        def v410CL = MavenClassLoader.forBookKeeperVersion(VERSION_4_1_x)
        def v410BK = v410CL.newBookKeeper(zookeeper)
        def currentCL = MavenClassLoader.forBookKeeperVersion(currentVersion)
        def currentBK = currentCL.newBookKeeper(zookeeper)
        try {
            def ledger0 = v410BK.createLedger(3, 2,
                                              v410CL.digestType("CRC32"),
                                              PASSWD)
            for (int i = 0; i < numEntries; i++) {
                ledger0.addEntry(("foobar" + i).getBytes())
            }
            ledger0.close()

            // Current client shouldn't be able to write to 4.1.0 server
            def ledger1 = currentBK.createLedger(3, 2, currentCL.digestType("CRC32"), PASSWD)
            try {
                ledger1.addEntry("foobar".getBytes())

                Assert.fail("Shouldn't have been able to write")
            } catch (Exception e) {
                // correct behaviour
            }

            Assert.assertTrue(BookKeeperClusterUtils.stopAllBookies(docker))
            Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, currentVersion))

            // check that old client can read its old ledgers on new server
            def ledger2 = v410BK.openLedger(ledger0.getId(), v410CL.digestType("CRC32"), PASSWD)
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
            currentBK.close()
            currentCL.close()
            v410BK.close()
            v410CL.close()
        }
    }

    @Test
    public void test9_v410ClientCantFenceLedgerFromCurrent() throws Exception {
        String currentVersion = BookKeeperClusterUtils.CURRENT_VERSION
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker)

        def currentCL = MavenClassLoader.forBookKeeperVersion(currentVersion)
        def currentBK = currentCL.newBookKeeper(zookeeper)
        def v410CL = MavenClassLoader.forBookKeeperVersion(VERSION_4_1_x)
        def v410BK = v410CL.newBookKeeper(zookeeper)

        try {
            def numEntries = 5
            def ledger0 = currentBK.createLedger(3, 2,
                                                 currentCL.digestType("CRC32"),
                                                 PASSWD)
            for (int i = 0; i < numEntries; i++) {
                ledger0.addEntry(("foobar" + i).getBytes())
            }
            ledger0.close()

            try {
                def ledger1 = v410BK.openLedger(ledger0.getId(), v410CL.digestType("CRC32"), PASSWD)
                Assert.fail("Shouldn't have been able to open")
            } catch (Exception e) {
                // correct behaviour
            }
        } finally {
            v410BK.close()
            v410CL.close()
            currentBK.close()
            currentCL.close()
        }
    }
}
