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

@RunWith(Arquillian.class)
class TestCompatOldClients {
    private static byte[] PASSWD = "foobar".getBytes()

    @ArquillianResource
    DockerClient docker

    private String currentVersion = BookKeeperClusterUtils.CURRENT_VERSION

    @Before
    public void before() throws Exception {
        Assert.assertTrue(BookKeeperClusterUtils.stopAllBookies(docker))
        // First test to run, formats metadata and bookies
        if (BookKeeperClusterUtils.metadataFormatIfNeeded(docker, currentVersion)) {
            BookKeeperClusterUtils.formatAllBookies(docker, currentVersion)
        }
        // If already started, this has no effect
        Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, currentVersion))
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

    private void testReadOpenFailure(String writeVersion, String readerVersion, boolean expectFail) throws Exception {
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
                if (expectFail) {
                    Assert.fail("For older versions Openledger call is expected to fail with ZKException, writerVersion: " + writeVersion + ", readerVersion: " + readerVersion)
                }
            } catch (Exception exc) {
                if (!expectFail) {
                    Assert.fail("For older versions Openledger call is expected to work, writerVersion: " + writeVersion + ", readerVersion: " + readerVersion)
                }
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
        BookKeeperClusterUtils.OLD_CLIENT_VERSIONS.each{
            def version = it
            ThreadReaper.runWithReaper({ testReadOpenFailure(currentVersion, version, !BookKeeperClusterUtils.hasVersionLatestMetadataFormat(version)) })
        }
    }

    @Test
    public void testNewClientReadsNewClient() throws Exception {
        BookKeeperClusterUtils.OLD_CLIENT_VERSIONS.each{
            def version = it
            ThreadReaper.runWithReaper({ testReads(version, currentVersion) })
        }
    }
}
