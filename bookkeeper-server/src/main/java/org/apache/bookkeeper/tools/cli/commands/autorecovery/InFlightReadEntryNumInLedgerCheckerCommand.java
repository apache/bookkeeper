/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.tools.cli.commands.autorecovery;

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Command to Setter and Getter for InFlightReadEntryNumInLedgerChecker value in metadata store.
 */
public class InFlightReadEntryNumInLedgerCheckerCommand extends
        BookieCommand<InFlightReadEntryNumInLedgerCheckerCommand.IFRENFlags> {

    static final Logger LOG = LoggerFactory.getLogger(InFlightReadEntryNumInLedgerCheckerCommand.class);

    private static final String NAME = "inflightreadentrynuminledgerchecker";
    private static final String DESC =
            "Setter and Getter for InFlightReadEntryNumInLedgerChecker value in metadata store";

    private static final int DEFAULT = -1;

    public InFlightReadEntryNumInLedgerCheckerCommand() {
        this(new IFRENFlags());
    }

    private InFlightReadEntryNumInLedgerCheckerCommand(IFRENFlags flags) {
        super(CliSpec.<IFRENFlags>newBuilder()
                .withName(NAME)
                .withDescription(DESC)
                .withFlags(flags)
                .build());
    }

    /**
     * Flags for command InFlightReadEntryNumInLedgerChecker.
     */
    @Accessors(fluent = true)
    @Setter
    public static class IFRENFlags extends CliFlags {

        @Parameter(names = { "-g", "--get" }, description = "Get InFlightReadEntryNumInLedgerChecker value")
        private boolean get;

        @Parameter(names = { "-s", "--set" }, description = "Set InFlightReadEntryNumInLedgerChecker value")
        private int set = DEFAULT;

    }

    @Override
    public boolean apply(ServerConfiguration conf, IFRENFlags cmdFlags) {
        try {
            return handler(conf, cmdFlags);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    public boolean handler(ServerConfiguration conf, IFRENFlags flags)
            throws InterruptedException, BKException, IOException, ReplicationException.UnavailableException,
            ReplicationException.CompatibilityException, KeeperException {
        boolean getter = flags.get;
        boolean setter = false;
        if (flags.set != DEFAULT) {
            setter = true;
        }

        if ((!getter && !setter) || (getter && setter)) {
            LOG.error("One and only one of -get and -set must be specified");
            return false;
        }
        ClientConfiguration adminConf = new ClientConfiguration(conf);
        BookKeeperAdmin admin = new BookKeeperAdmin(adminConf);
        try {
            if (getter) {
                int inFlightReadEntryNum = admin.getInFlightReadEntryNumInLedgerChecker();
                LOG.info("InFlightReadEntryNumInLedgerChecker value in ZK: {}", inFlightReadEntryNum);
            } else {
                int inFlightReadEntryNum = flags.set;
                admin.setInFlightReadEntryNumInLedgerChecker(inFlightReadEntryNum);
                LOG.info("Successfully set InFlightReadEntryNumInLedgerChecker value in ZK: {}",
                        inFlightReadEntryNum);
            }
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
        return true;
    }
}
