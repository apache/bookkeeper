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
import org.apache.bookkeeper.tests.integration.utils.ThreadReaper

import org.jboss.arquillian.junit.Arquillian
import org.jboss.arquillian.test.api.ArquillianResource

import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(Arquillian.class)
class TestCompatOldClients {
    private static final Logger LOG = LoggerFactory.getLogger(TestCompatOldClients.class)
    private static byte[] PASSWD = "foobar".getBytes()

    // 4.1.0 doesn't work because metadata format changed
    private def oldClientVersions = ["4.4.0", "4.5.1", "4.6.2", "4.7.2", "4.8.2", "4.9.2" ]

    @ArquillianResource
    DockerClient docker

    private String currentVersion = System.getProperty("currentVersion")

    @Before
    public void before() throws Exception {
        // First test to run, formats metadata and bookies
        if (BookKeeperClusterUtils.metadataFormatIfNeeded(docker, currentVersion)) {
            BookKeeperClusterUtils.formatAllBookies(docker, currentVersion)
        }
        // If already started, this has no effect
        Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, currentVersion))
    }

    private void testFencingOldClient(String oldClientVersion, String fencingVersion) {
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker)

        def oldCL = MavenClassLoader.forBookKeeperVersion(oldClientVersion)
        def oldBK = oldCL.newBookKeeper(zookeeper)
        def fencingCL = MavenClassLoader.forBookKeeperVersion(fencingVersion)
        def fencingBK = fencingCL.newBookKeeper(zookeeper)

        try {
            def numEntries = 5
            def ledger0 = oldBK.createLedger(3, 2,
                                             oldCL.digestType("CRC32"),
                                             PASSWD)
            for (int i = 0; i < numEntries; i++) {
                ledger0.addEntry(("foobar" + i).getBytes())
            }
            ledger0.close()


            def ledger1 = fencingBK.openLedger(ledger0.getId(), fencingCL.digestType("CRC32"), PASSWD)

            // cannot write any more
            try {
                ledger0.addEntry("shouldn't work".getBytes())
                Assert.fail("Shouldn't have been able to add any more")
            } catch (Exception e) {
                Assert.assertEquals(e.getClass().getName(),
                                    "org.apache.bookkeeper.client.BKException\$BKLedgerClosedException")
            }

            // should be able to open it and read it back
            def ledger2 = oldBK.openLedger(ledger0.getId(), oldCL.digestType("CRC32"), PASSWD)
            def entries = ledger2.readEntries(0, ledger2.getLastAddConfirmed())
            Assert.assertEquals(numEntries, ledger2.getLastAddConfirmed() + 1 /* counts from 0 */)
            int j = 0
            while (entries.hasMoreElements()) {
                def e = entries.nextElement()
                Assert.assertEquals(new String(e.getEntry()), "foobar"+ j)
                j++
            }
            ledger2.close()
        } finally {
            oldBK.close()
            oldCL.close()

            fencingBK.close()
            fencingCL.close()
        }
    }

    @Test
    public void testNewClientFencesOldClient() throws Exception {
        oldClientVersions.each{
            def version = it
            ThreadReaper.runWithReaper({ testFencingOldClient(version, currentVersion) })
        }
    }

    @Test
    public void testOldClientFencesOldClient() throws Exception {
        oldClientVersions.each{
            def version = it
            ThreadReaper.runWithReaper({ testFencingOldClient(version, version) })
        }
    }

    private void testReads(String writeVersion, String readerVersion) throws Exception {
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker)

        def writeCL = MavenClassLoader.forBookKeeperVersion(writeVersion)
        def writeBK = writeCL.newBookKeeper(zookeeper)
        def readCL = MavenClassLoader.forBookKeeperVersion(readerVersion)
        def readBK = readCL.newBookKeeper(zookeeper)
        try {
            def numEntries = 5
            def ledger0 = writeBK.createLedger(3, 2,
                                               writeCL.digestType("CRC32"),
                                               PASSWD)
            for (int i = 0; i < numEntries; i++) {
                ledger0.addEntry(("foobar" + i).getBytes())
            }
            ledger0.close()


            def ledger1 = readBK.openLedger(ledger0.getId(), readCL.digestType("CRC32"), PASSWD)

            def entries = ledger1.readEntries(0, ledger1.getLastAddConfirmed())
            Assert.assertEquals(numEntries, ledger1.getLastAddConfirmed() + 1 /* counts from 0 */)
            int j = 0
            while (entries.hasMoreElements()) {
                def e = entries.nextElement()
                Assert.assertEquals(new String(e.getEntry()), "foobar"+ j)
                j++
            }
            ledger1.close()
        } finally {
            readBK.close()
            readCL.close()
            writeBK.close()
            writeCL.close()
        }
    }

    private void testReadOpenFailure(String writeVersion, String readerVersion) throws Exception {
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker)

        def writeCL = MavenClassLoader.forBookKeeperVersion(writeVersion)
        def writeBK = writeCL.newBookKeeper(zookeeper)
        def readCL = MavenClassLoader.forBookKeeperVersion(readerVersion)
        def readBK = readCL.newBookKeeper(zookeeper)
        try {
            def numEntries = 5
            def ledger0 = writeBK.createLedger(3, 2,
                                               writeCL.digestType("CRC32"),
                                               PASSWD)
            for (int i = 0; i < numEntries; i++) {
                ledger0.addEntry(("foobar" + i).getBytes())
            }
            ledger0.close()

            try {
                def ledger1 = readBK.openLedger(ledger0.getId(), readCL.digestType("CRC32"), PASSWD)
                Assert.fail("For older versions Openledger call is expected to fail with ZKException");
            } catch (Exception exc) {
                Assert.assertEquals(exc.getClass().getName(),
                                "org.apache.bookkeeper.client.BKException\$ZKException")
            }
        } finally {
            readBK.close()
            readCL.close()
            writeBK.close()
            writeCL.close()
        }
    }

    /**
     * Since METADATA_VERSION is upgraded and it is using binary format, the older
     * clients which are expecting text format would fail to read ledger metadata.
     */
    @Test
    public void testOldClientReadsNewClient() throws Exception {
        oldClientVersions.each{
            def version = it
            ThreadReaper.runWithReaper({ testReadOpenFailure(currentVersion, version) })
        }
    }

    @Test
    public void testNewClientReadsNewClient() throws Exception {
        oldClientVersions.each{
            def version = it
            ThreadReaper.runWithReaper({ testReads(version, currentVersion) })
        }
    }
}
