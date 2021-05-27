/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithLedgerManagerFactory;

import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Test;


/**
 * Integration test of {@link BookieShell.TriggerAuditCmd}.
 */
public class ForceAuditorChecksCmdTest extends BookKeeperClusterTestCase {

    public ForceAuditorChecksCmdTest() {
        super(1);
        baseConf.setAuditorPeriodicPlacementPolicyCheckInterval(10000);
        baseConf.setAuditorPeriodicReplicasCheckInterval(10000);
    }

    /**
     * Verify that the auditor checks last execution time (stored in zookeeper) is reset to an older value
     * when triggeraudit command is run with certain parameters. Rebooting the auditor after this would
     * result in immediate run of audit checks.
     */
    @Test
    public void verifyAuditCTimeReset() throws Exception {
        String[] argv = new String[] { "forceauditchecks", "-calc", "-ppc", "-rc" };
        long curTime = System.currentTimeMillis();

        final ServerConfiguration conf = confByIndex(0);
        BookieShell bkShell = new BookieShell();
        bkShell.setConf(conf);

        // Add dummy last execution time for audit checks
        runFunctionWithLedgerManagerFactory(conf, mFactory -> {
            try (LedgerUnderreplicationManager urM =
                         mFactory.newLedgerUnderreplicationManager()) {
                urM.setCheckAllLedgersCTime(curTime);
                urM.setPlacementPolicyCheckCTime(curTime);
                urM.setReplicasCheckCTime(curTime);
            } catch (InterruptedException | KeeperException | ReplicationException e) {
                throw new UncheckedExecutionException(e);
            }
            return null;
        });

        // Run the actual shell command
        Assert.assertEquals("Failed to return exit code!", 0, bkShell.run(argv));

        // Verify that the time has been reset to an older value (at least 20 days)
        runFunctionWithLedgerManagerFactory(conf, mFactory -> {
            try (LedgerUnderreplicationManager urm =
                         mFactory.newLedgerUnderreplicationManager()) {
                long checkAllLedgersCTime = urm.getCheckAllLedgersCTime();
                if (checkAllLedgersCTime > (curTime - (20 * 24 * 60 * 60 * 1000))) {
                    Assert.fail("The checkAllLedgersCTime should have been reset to atleast 20 days old");
                }
                long placementPolicyCheckCTime = urm.getPlacementPolicyCheckCTime();
                if (placementPolicyCheckCTime > (curTime - (20 * 24 * 60 * 60 * 1000))) {
                    Assert.fail("The placementPolicyCheckCTime should have been reset to atleast 20 days old");
                }
                long replicasCheckCTime = urm.getReplicasCheckCTime();
                if (replicasCheckCTime > (curTime - (20 * 24 * 60 * 60 * 1000))) {
                    Assert.fail("The replicasCheckCTime should have been reset to atleast 20 days old");
                }
            } catch (InterruptedException | KeeperException | ReplicationException e) {
                throw new UncheckedExecutionException(e);
            }
            return null;
        });
    }
}
