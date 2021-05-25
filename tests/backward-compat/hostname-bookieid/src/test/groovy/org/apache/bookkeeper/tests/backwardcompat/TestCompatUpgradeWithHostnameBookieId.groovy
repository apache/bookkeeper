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
import org.junit.Test
import org.junit.runner.RunWith

import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(Arquillian.class)
class TestCompatUpgradeWithHostnameBookieId {
    private static final Logger LOG = LoggerFactory.getLogger(TestCompatUpgradeWithHostnameBookieId.class)
    private static byte[] PASSWD = "foobar".getBytes()

    private def oldClientVersions = ["4.4.0", "4.5.1", "4.6.2", "4.7.2", "4.8.2", "4.9.2",
                                     "4.10.0", "4.11.1", "4.12.1", "4.13.0", "4.14.0" ]

    @ArquillianResource
    DockerClient docker


    private void writeEntries(def ledger, int numEntries) throws Exception {
        for (int i = 0; i < numEntries; i++) {
            ledger.addEntry(("foobar" + i).getBytes())
        }
    }

    private void assertHasEntries(def ledger, int numEntries) throws Exception {
        Assert.assertEquals(numEntries, ledger.getLastAddConfirmed() + 1 /* counts from 0 */)
        def entries = ledger.readEntries(0, ledger.getLastAddConfirmed())
        int j = 0
        while (entries.hasMoreElements()) {
            def e = entries.nextElement()
            Assert.assertEquals(new String(e.getEntry()), "foobar"+ j)
            j++
        }
    }

    /**
     * Test compatability between version old version and the current version.
     * - 4.1.0 server restarts with useHostNameAsBookieID=true.
     * - Write ledgers with old and new clients
     * - Read ledgers written by old clients.
     */
    @Test
    public void testOldClientsWorkWithNewServerUsingHostnameAsBookieID() throws Exception {
        int numEntries = 10
        BookKeeperClusterUtils.legacyMetadataFormat(docker)
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker)

        String currentVersion = System.getProperty("currentVersion")

        Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, "4.1.0"))

        def v410CL = MavenClassLoader.forBookKeeperVersion("4.1.0")
        def v410BK = v410CL.newBookKeeper(zookeeper)
        def v420CL = MavenClassLoader.forBookKeeperVersion("4.2.0")
        def v420BK = v420CL.newBookKeeper(zookeeper)
        def currentCL = MavenClassLoader.forBookKeeperVersion(currentVersion)
        def currentBK = currentCL.newBookKeeper(zookeeper)

        try {
            // Write a ledger with v4.1.0 client
            def ledger410 = v410BK.createLedger(3, 2, v410CL.digestType("CRC32"), PASSWD)
            writeEntries(ledger410, numEntries)
            ledger410.close()

            // Write a ledger with v4.2.0 client
            def ledger420 = v420BK.createLedger(3, 2, v420CL.digestType("CRC32"), PASSWD)
            writeEntries(ledger420, numEntries)
            ledger420.close()

            // Stop bookies, change config to use hostname as id, restart with latest version
            Assert.assertTrue(BookKeeperClusterUtils.stopAllBookies(docker))
            BookKeeperClusterUtils.updateAllBookieConf(docker, currentVersion, "useHostNameAsBookieID", "true")
            Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, currentVersion))

            // Ensure we can read ledger with v4.1.0 client
            def ledger410r = v410BK.openLedger(ledger410.getId(), v410CL.digestType("CRC32"), PASSWD)
            assertHasEntries(ledger410r, numEntries)
            ledger410r.close()

            // Ensure we can read ledger with v4.2.0 client
            def ledger420r = v420BK.openLedger(ledger420.getId(), v420CL.digestType("CRC32"), PASSWD)
            assertHasEntries(ledger420r, numEntries)
            ledger420r.close()

            // Ensure we can write and read new ledgers with all client versions
            oldClientVersions.each{
                LOG.info("Testing ledger creation for version {}", it)
                def oldCL = MavenClassLoader.forBookKeeperVersion(it)
                def oldBK = oldCL.newBookKeeper(zookeeper)
                try {
                    def ledger0 = oldBK.createLedger(3, 2, oldCL.digestType("CRC32"), PASSWD)
                    writeEntries(ledger0, numEntries)
                    ledger0.close()

                    def ledger1 = currentBK.openLedger(ledger0.getId(), currentCL.digestType("CRC32"), PASSWD)
                    assertHasEntries(ledger1, numEntries)
                    ledger1.close()
                } finally {
                    oldBK.close()
                    oldCL.close()
                }
            }
        } finally {
            currentBK.close()
            currentCL.close()
            v420BK.close()
            v420CL.close()
            v410BK.close()
            v410CL.close()
        }
    }


}
