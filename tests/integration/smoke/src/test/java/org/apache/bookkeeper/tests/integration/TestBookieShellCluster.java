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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;

import com.github.dockerjava.api.DockerClient;
import java.util.concurrent.ExecutionException;
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

    private static final byte[] PASSWORD = "foobar".getBytes(UTF_8);

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

    private static long writeNEntries(BookKeeper bk, int n, int numBookies) throws Exception {
        try (WriteHandle writer = bk.newCreateLedgerOp().withEnsembleSize(numBookies)
             .withWriteQuorumSize(numBookies).withAckQuorumSize(numBookies)
             .withPassword(PASSWORD).execute().get()) {
            int i = 0;
            for (; i < n - 1; i++) {
                writer.appendAsync(("entry" + i).getBytes(UTF_8));
            }
            writer.append(("entry" + i).getBytes(UTF_8));

            return writer.getId();
        }
    }

    private static void validateNEntries(BookKeeper bk, long ledgerId, int n) throws Exception {
        try (ReadHandle reader = bk.newOpenLedgerOp()
             .withLedgerId(ledgerId)
             .withPassword(PASSWORD)
             .execute().get();
             LedgerEntries entries = reader.read(0, n - 1)) {
            Assert.assertEquals(reader.getLastAddConfirmed(), n - 1);

            for (int i = 0; i < n; i++) {
                Assert.assertEquals("entry" + i, new String(entries.getEntry(i).getEntryBytes(), UTF_8));
            }
        }
    }

    /**
     * These tests on being able to access cluster internals, so can't be put in test base.
     */
    @Test
    public void test101_RegenerateIndex() throws Exception {
        String zookeeper = String.format("zk+hierarchical://%s/ledgers",
                                         BookKeeperClusterUtils.zookeeperConnectString(docker));
        int numEntries = 100;

        try (BookKeeper bk = BookKeeper.newBuilder(
                     new ClientConfiguration().setMetadataServiceUri(zookeeper)).build()) {
            log.info("Writing entries");
            long ledgerId1 = writeNEntries(bk, numEntries, BookKeeperClusterUtils.allBookies().size());
            long ledgerId2 = writeNEntries(bk, numEntries, BookKeeperClusterUtils.allBookies().size());

            log.info("Validate that we can read back");
            validateNEntries(bk, ledgerId1, numEntries);
            validateNEntries(bk, ledgerId2, numEntries);

            String indexFileName1 = String.format("/opt/bookkeeper/data/ledgers/current/0/%d/%d.idx",
                                                  ledgerId1, ledgerId1);
            String indexFileName2 = String.format("/opt/bookkeeper/data/ledgers/current/0/%d/%d.idx",
                                                  ledgerId2, ledgerId2);

            log.info("Stop bookies to flush, delete the index and start again");
            assertTrue(BookKeeperClusterUtils.stopAllBookies(docker));

            BookKeeperClusterUtils.runOnAllBookies(docker, "rm", indexFileName1, indexFileName2);
            assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, currentVersion));

            log.info("Validate that we cannot read back");
            try {
                validateNEntries(bk, ledgerId1, numEntries);
                Assert.fail("Shouldn't have been able to find anything");
            } catch (BKException.BKNoSuchLedgerExistsException e) {
                // expected
            }
            try {
                validateNEntries(bk, ledgerId2, numEntries);
                Assert.fail("Shouldn't have been able to find anything");
            } catch (BKException.BKNoSuchLedgerExistsException e) {
                // expected
            }

            assertTrue(BookKeeperClusterUtils.stopAllBookies(docker));

            log.info("Regenerate the index file");
            BookKeeperClusterUtils.runOnAllBookies(docker,
                    bkScript, "shell", "regenerate-interleaved-storage-index-file",
                    "--ledgerIds", String.format("%d,%d", ledgerId1, ledgerId2),
                    "--password", new String(PASSWORD, UTF_8));
            assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, currentVersion));

            log.info("Validate that we can read back, after regeneration");
            validateNEntries(bk, ledgerId1, numEntries);
            validateNEntries(bk, ledgerId2, numEntries);
        }
    }

    @Test
    public void test102_DumpRestoreMetadata() throws Exception {
        String zookeeper = String.format("zk+hierarchical://%s/ledgers",
                                         BookKeeperClusterUtils.zookeeperConnectString(docker));
        int numEntries = 100;

        try (BookKeeper bk = BookKeeper.newBuilder(
                     new ClientConfiguration().setMetadataServiceUri(zookeeper)).build()) {
            log.info("Writing entries");
            long ledgerId = writeNEntries(bk, numEntries, 1);

            log.info("Dumping ledger metadata to file");
            String bookie = BookKeeperClusterUtils.getAnyBookie();
            String dumpFile = String.format("/tmp/ledger-%d-%d", ledgerId, System.nanoTime());
            DockerUtils.runCommand(docker, bookie,
                                   bkScript, "shell", "ledgermetadata",
                                   "--ledgerid", String.valueOf(ledgerId),
                                   "--dumptofile", dumpFile);

            log.info("Delete the ledger metadata");
            bk.newDeleteLedgerOp().withLedgerId(ledgerId).execute().get();

            // hopefully ledger gc doesn't kick in
            log.info("Verify that we cannot open ledger");
            try {
                validateNEntries(bk, ledgerId, numEntries);
                Assert.fail("Shouldn't have been able to find anything");
            } catch (ExecutionException ee) {
                Assert.assertEquals(ee.getCause().getClass(), BKException.BKNoSuchLedgerExistsException.class);
            }

            log.info("Restore the ledger metadata");
            DockerUtils.runCommand(docker, bookie,
                                   bkScript, "shell", "ledgermetadata",
                                   "--ledgerid", String.valueOf(ledgerId),
                                   "--restorefromfile", dumpFile);

            log.info("Validate that we can read back, after regeneration");
            validateNEntries(bk, ledgerId, numEntries);
        }
    }

}
