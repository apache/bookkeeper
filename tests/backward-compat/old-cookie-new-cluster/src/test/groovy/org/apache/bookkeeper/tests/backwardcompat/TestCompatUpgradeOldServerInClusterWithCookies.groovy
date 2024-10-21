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
class TestCompatUpgradeOldServerInClusterWithCookies {
    private static final Logger LOG = LoggerFactory.getLogger(TestCompatUpgradeOldServerInClusterWithCookies.class)
    private static byte[] PASSWD = "foobar".getBytes()

    @ArquillianResource
    DockerClient docker

    @Test
    public void testUpgradeServerToClusterWithCookies() throws Exception {
        BookKeeperClusterUtils.legacyMetadataFormat(docker)
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker)

        String currentVersion = BookKeeperClusterUtils.CURRENT_VERSION
        int numEntries = 10

        Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, "4.1.0"))
        def v410CL = MavenClassLoader.forBookKeeperVersion("4.1.0")
        def v410BK = v410CL.newBookKeeper(zookeeper)
        try {
            def ledger0 = v410BK.createLedger(3, 2, v410CL.digestType("CRC32"), PASSWD)
            for (int i = 0; i < numEntries; i++) {
                ledger0.addEntry(("foobar" + i).getBytes())
            }
            ledger0.close()

            Assert.assertTrue(BookKeeperClusterUtils.stopAllBookies(docker))

            // format metadata
            String bookieScript = "/opt/bookkeeper/" + currentVersion + "/bin/bookkeeper"
            Assert.assertTrue(
                BookKeeperClusterUtils.runOnAnyBookie(docker, bookieScript,
                                                      "shell", "metaformat", "-nonInteractive", "-force"))

            // bookies shouldn't come up because the cookie doesn't have instance id
            Assert.assertFalse(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, currentVersion))

            // format bookie
            BookKeeperClusterUtils.runOnAllBookies(docker, bookieScript,
                                                   "shell", "bookieformat", "-nonInteractive", "-force")

            // bookies should come up
            Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, currentVersion))

            // but data has been lost of course, we formatted everything
            try {
                v410BK.openLedger(ledger0.getId(), v410CL.digestType("CRC32"), PASSWD)
            } catch (Exception e) {
                // correct behaviour
            }
        } finally {
            v410BK.close()
            v410CL.close()
        }
    }
}
