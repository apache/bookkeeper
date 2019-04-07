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

package org.apache.bookkeeper.tests.integration;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.github.dockerjava.api.DockerClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.tests.integration.utils.BookKeeperClusterUtils;
import org.apache.bookkeeper.tests.integration.utils.DockerUtils;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

/**
 * Test `bin/bkctl`.
 */
@Slf4j
@RunWith(Arquillian.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestCLI {

    @ArquillianResource
    private DockerClient docker;

    private String currentVersion = System.getProperty("currentVersion");
    private String bkctl;

    @Before
    public void setup() {
        bkctl = "/opt/bookkeeper/" + currentVersion + "/bin/bkctl";
    }

    @Test
    public void test000_Setup() throws Exception {
        // First test to run, formats metadata and bookies
        if (BookKeeperClusterUtils.metadataFormatIfNeeded(docker, currentVersion)) {
            BookKeeperClusterUtils.formatAllBookies(docker, currentVersion);
        }
        assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, currentVersion));

    }

    @Test
    public void test999_Teardown() throws Exception {
        assertTrue(BookKeeperClusterUtils.stopAllBookies(docker));
    }

    @Test
    public void test001_SimpleTest() throws Exception {
        String bookie = BookKeeperClusterUtils.getAnyBookie();
        assertTrue(DockerUtils.runCommand(docker, bookie,
            bkctl,
            "ledger",
            "simpletest",
            "--ensemble-size", "3",
            "--write-quorum-size", "3",
            "--ack-quorum-size", "2",
            "--num-entries", "100"
        ).contains("100 entries written to ledger"));
    }

    @Test
    public void test002_ListROBookies() throws Exception {
        String bookie = BookKeeperClusterUtils.getAnyBookie();
        assertTrue(DockerUtils.runCommand(docker, bookie,
            bkctl,
            "bookies",
            "list",
            "-ro"
        ).contains("No bookie exists!"));
    }

    @Test
    public void test003_ListRWBookies() throws Exception {
        String bookie = BookKeeperClusterUtils.getAnyBookie();
        assertTrue(DockerUtils.runCommand(docker, bookie,
            bkctl,
            "bookies",
            "list",
            "-rw"
        ).contains("ReadWrite Bookies :"));
    }

    @Test
    public void test004_SearchReplaceBookieId() throws Exception {
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker);

        String bookie = BookKeeperClusterUtils.getAnyBookie();
        int numEntries = 100;
        try (BookKeeper bk = new BookKeeper(zookeeper)) {
            long ledgerId;
            BookieSocketAddress toReplace;
            BookieSocketAddress replaceWith = new BookieSocketAddress("192.0.2.1:3181");
            try (WriteHandle writelh = bk.newCreateLedgerOp()
                    .withDigestType(DigestType.CRC32C).withPassword(TestSmoke.PASSWD)
                    .withEnsembleSize(1).withWriteQuorumSize(1).withAckQuorumSize(1).execute().get()) {
                ledgerId = writelh.getId();
                toReplace = writelh.getLedgerMetadata().getAllEnsembles().get(0L).get(0);
                for (int i = 0; i < numEntries; i++) {
                    writelh.append(("entry-" + i).getBytes());
                }
            }

            TestSmoke.readEntries(bk, ledgerId, numEntries);

            DockerUtils.runCommand(docker, bookie,
                                   bkctl,
                                   "bookieid", "searchreplace",
                                   "--from", toReplace.toString(),
                                   "--to", replaceWith.toString());

            try {
                TestSmoke.readEntries(bk, ledgerId, numEntries);
                fail("Shouldn't be able to read, as bookie id is rubbish");
            } catch (BKException.BKBookieHandleNotAvailableException e) {
                // expected
            }

            DockerUtils.runCommand(docker, bookie,
                                   bkctl,
                                   "bookieid", "searchreplace",
                                   "--from", replaceWith.toString(),
                                   "--to", toReplace.toString());
            TestSmoke.readEntries(bk, ledgerId, numEntries);
        }
    }
}
