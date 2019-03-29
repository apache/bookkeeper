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
package org.apache.bookkeeper.tools.cli.commands.autorecovery;

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.concurrent.ExecutionException;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Command to enable or disable auto recovery in the cluster.
 */
public class ToggleCommand extends BookieCommand<ToggleCommand.AutoRecoveryFlags> {

    static final Logger LOG = LoggerFactory.getLogger(ToggleCommand.class);

    private static final String NAME = "toggle";
    private static final String DESC = "Enable or disable auto recovery in the cluster. Default is disable.";

    public ToggleCommand() {
        this(new AutoRecoveryFlags());
    }

    private ToggleCommand(AutoRecoveryFlags flags) {
        super(CliSpec.<ToggleCommand.AutoRecoveryFlags>newBuilder()
            .withName(NAME).withDescription(DESC)
            .withFlags(flags).build());
    }

    /**
     * Flags for auto recovery command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class AutoRecoveryFlags extends CliFlags {

        @Parameter(names = { "-e", "--enable" }, description = "Enable or disable auto recovery of under replicated "
                                                               + "ledgers.")
        private boolean enable;

        @Parameter(names = {"-s", "--status"}, description = "Check the auto recovery status.")
        private boolean status;

    }

    @Override
    public boolean apply(ServerConfiguration conf, AutoRecoveryFlags cmdFlags) {
        try {
            return handler(conf, cmdFlags);
        } catch (MetadataException | ExecutionException e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private boolean handler(ServerConfiguration conf, AutoRecoveryFlags flags)
        throws MetadataException, ExecutionException {
        MetadataDrivers.runFunctionWithLedgerManagerFactory(conf, mFactory -> {
            try {
                try (LedgerUnderreplicationManager underreplicationManager = mFactory
                         .newLedgerUnderreplicationManager()) {
                    if (flags.status) {
                        System.out.println("Autorecovery is " + (underreplicationManager.isLedgerReplicationEnabled()
                                                                     ? "enabled." : "disabled."));
                        return null;
                    }
                    if (flags.enable) {
                        if (underreplicationManager.isLedgerReplicationEnabled()) {
                            LOG.warn("Autorecovery already enabled. Doing nothing");
                        } else {
                            LOG.info("Enabling autorecovery");
                            underreplicationManager.enableLedgerReplication();
                        }
                    } else {
                        if (!underreplicationManager.isLedgerReplicationEnabled()) {
                            LOG.warn("Autorecovery already disabled. Doing nothing");
                        } else {
                            LOG.info("Disabling autorecovery");
                            underreplicationManager.disableLedgerReplication();
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UncheckedExecutionException(e);
            } catch (KeeperException | ReplicationException e) {
                throw new UncheckedExecutionException(e);
            }
            return null;
        });
        return true;
    }
}
