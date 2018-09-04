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

import com.github.dockerjava.api.DockerClient;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.tests.integration.utils.BookKeeperClusterUtils;
import org.apache.bookkeeper.tests.integration.utils.DockerUtils;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

/**
 * Test cluster related commands in bookie shell.
 */
@Slf4j
@RunWith(Arquillian.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestBookieShellCluster extends BookieShellTestBase {

    @ArquillianResource
    private DockerClient docker;

    private String currentVersion = System.getProperty("currentVersion");

    @Test
    @Override
    public void test000_Setup() throws Exception {
        // First test to run, formats metadata and bookies
        if (BookKeeperClusterUtils.metadataFormatIfNeeded(docker, currentVersion)) {
            BookKeeperClusterUtils.formatAllBookies(docker, currentVersion);
        }
        assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, currentVersion));
    }

    @Test
    @Override
    public void test999_Teardown() {
        assertTrue(BookKeeperClusterUtils.stopAllBookies(docker));
    }

    @Override
    protected String runCommandInAnyContainer(String... cmds) throws Exception {
        String bookie = BookKeeperClusterUtils.getAnyBookie();
        return DockerUtils.runCommand(docker, bookie, cmds);
    }

    @Test
    @Override
    public void test001_SimpleTest() throws Exception {
        super.test001_SimpleTest();
    }

    @Test
    @Override
    public void test002_ListROBookies() throws Exception {
        super.test002_ListROBookies();
    }

    @Test
    @Override
    public void test003_ListRWBookies() throws Exception {
        super.test003_ListRWBookies();
    }

    /**
     * These tests on being able to access cluster internals, so can't be put in test base.
     */
    @Test
    public void test101_RegenerateIndex() throws Exception {
        String zookeeper = String.format("zk+hierarchical://%s/ledgers",
                                         BookKeeperClusterUtils.zookeeperConnectString(docker));
        int numEntries = 100;

        long ledgerId = 0;


        try (BookKeeper bk = BookKeeper.newBuilder(
                     new ClientConfiguration().setMetadataServiceUri(zookeeper)).build()) {
            log.info("Writing entries");
            try (WriteHandle writer = bk.newCreateLedgerOp().withEnsembleSize(1)
                    .withWriteQuorumSize(1).withAckQuorumSize(1)
                    .withPassword("".getBytes()).execute().get()) {
                int i = 0;
                for (; i < numEntries - 1; i++) {
                    writer.appendAsync(("entry" + i).getBytes());
                }
                writer.append(("entry" + i).getBytes());

                ledgerId = ledgerId;
            }

            log.info("Restart bookies so index gets flushed");
            assertTrue(BookKeeperClusterUtils.stopAllBookies(docker));
            assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, currentVersion));

            log.info("Find the bookie containing the index");
            String indexFileName = String.format("/opt/bookkeeper/data/ledgers/current/0/0/%d.idx", ledgerId);
            List<String> bookie = BookKeeperClusterUtils.allBookies().stream().filter(
                    (b) -> {
                        try {
                            DockerUtils.runCommand(docker, b, "test", "-f", indexFileName);
                            return true;
                        } catch (Exception e) {
                            return false;
                        }
                    }).collect(Collectors.toList());
            Assert.assertEquals(bookie.size(), 1); // should only be on one bookie

            log.info("Validate that we can read back");
            try (ReadHandle reader = bk.newOpenLedgerOp()
                    .withLedgerId(ledgerId)
                    .withPassword("".getBytes()).execute().get();
                 LedgerEntries entries = reader.read(0, numEntries - 1)) {
                Assert.assertEquals(reader.getLastAddConfirmed(), numEntries - 1);

                for (int i = 0; i < numEntries; i++) {
                    Assert.assertEquals(new String(entries.getEntry(i).getEntryBytes()), "entry" + i);
                }
            }

            log.info("Delete the index and restart to flush cache");
            assertTrue(BookKeeperClusterUtils.stopBookie(docker, bookie.get(0)));
            DockerUtils.runCommand(docker, bookie.get(0), "rm", indexFileName);
            assertTrue(BookKeeperClusterUtils.startBookieWithVersion(docker, bookie.get(0), currentVersion));

            log.info("Validate that we cannot read back");

            try (ReadHandle reader = bk.newOpenLedgerOp()
                    .withLedgerId(ledgerId)
                    .withPassword("".getBytes()).execute().get();
                 LedgerEntries entries = reader.read(0, numEntries - 1)) {
                Assert.fail("Shouldn't have been able to find anything");
            } catch (BKException.BKNoSuchLedgerExistsException e) {
                // expected
            }

            assertTrue(BookKeeperClusterUtils.stopBookie(docker, bookie.get(0)));
            log.info("Regenerate the index file");
            DockerUtils.runCommand(docker, bookie.get(0),
                                   bkScript, "shell", "regenerate-interleaved-storage-index-file",
                                   "--ledger", String.valueOf(ledgerId));
            assertTrue(BookKeeperClusterUtils.startBookieWithVersion(docker, bookie.get(0), currentVersion));

            log.info("Validate that we can read back, after regeneration");
            try (ReadHandle reader = bk.newOpenLedgerOp()
                    .withLedgerId(ledgerId)
                    .withPassword("".getBytes()).execute().get();
                 LedgerEntries entries = reader.read(0, numEntries - 1)) {
                Assert.assertEquals(reader.getLastAddConfirmed(), numEntries - 1);

                for (int i = 0; i < numEntries; i++) {
                    Assert.assertEquals(new String(entries.getEntry(i).getEntryBytes()), "entry" + i);
                }
            }
        }
    }

}
