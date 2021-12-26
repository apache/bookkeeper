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
class TestCompatHierarchicalLedgerManager {
    private static final Logger LOG = LoggerFactory.getLogger(TestCompatHierarchicalLedgerManager.class)
    private static byte[] PASSWD = "foobar".getBytes()

    @ArquillianResource
    DockerClient docker

    /**
     * Test compatability between version old version and the current version
     * with respect to the HierarchicalLedgerManagers.
     * - 4.2.0 server starts with HierarchicalLedgerManager.
     * - Write ledgers with old and new clients
     * - Read ledgers written by old clients.
     */
    @Test
    public void testCompatHierarchicalLedgerManagerV420toCurrent() throws Exception {
        String currentVersion = BookKeeperClusterUtils.CURRENT_VERSION
        BookKeeperClusterUtils.legacyMetadataFormat(docker)

        BookKeeperClusterUtils.updateAllBookieConf(docker, "4.2.0",
                                                   "ledgerManagerFactoryClass",
                                                   "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory")
        Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, "4.2.0"))

        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker)

        def v420CL = MavenClassLoader.forBookKeeperVersion("4.2.0")
        def v420BK = v420CL.newBookKeeper(zookeeper)
        def currentCL = MavenClassLoader.forBookKeeperVersion(currentVersion)
        def currentBK = currentCL.newBookKeeper(zookeeper)
        try {
            int numEntries = 10

            def ledger0 = v420BK.createLedger(3, 2, v420CL.digestType("CRC32"), PASSWD)
            for (int i = 0; i < numEntries; i++) {
                ledger0.addEntry(("foobar" + i).getBytes())
            }
            ledger0.close()

            Assert.assertTrue(BookKeeperClusterUtils.stopAllBookies(docker))

            BookKeeperClusterUtils.updateAllBookieConf(docker, currentVersion,
                                                       "ledgerManagerFactoryClass",
                                                       "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory")
            Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, currentVersion))

            def ledger1 = currentBK.openLedger(ledger0.getId(), currentCL.digestType("CRC32"), PASSWD)
            Assert.assertEquals(numEntries, ledger1.getLastAddConfirmed() + 1 /* counts from 0 */)
            def entries = ledger1.readEntries(0, ledger1.getLastAddConfirmed())
            int j = 0
            while (entries.hasMoreElements()) {
                def e = entries.nextElement()
                Assert.assertEquals(new String(e.getEntry()), "foobar"+ j)
                j++
            }
            ledger1.close()
        } finally {
            currentBK.close()
            currentCL.close()
            v420BK.close()
            v420CL.close()
        }
    }
}
