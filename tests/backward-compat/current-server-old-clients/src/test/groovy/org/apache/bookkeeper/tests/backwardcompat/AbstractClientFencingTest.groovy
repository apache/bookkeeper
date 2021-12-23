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
import org.jboss.arquillian.test.api.ArquillianResource
import org.junit.Assert
import org.junit.Before

abstract class AbstractClientFencingTest {
    protected static byte[] PASSWD = "foobar".getBytes()

    @ArquillianResource
    protected DockerClient docker

    protected String currentVersion = BookKeeperClusterUtils.CURRENT_VERSION

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

    protected void testFencingOldClient(String oldClientVersion, String fencingVersion) {
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
        } catch(Throwable tt) {
            throw new RuntimeException("Got exception with oldClientVersion: " + oldClientVersion + " and fencingVersion: " + fencingVersion, tt);
        } finally {
            oldBK.close()
            oldCL.close()

            fencingBK.close()
            fencingCL.close()
        }
    }
}
