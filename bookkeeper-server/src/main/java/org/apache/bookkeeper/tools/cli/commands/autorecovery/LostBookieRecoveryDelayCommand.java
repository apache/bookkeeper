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
import java.io.IOException;
import lombok.CustomLog;
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

/**
 * Command to Setter and Getter for LostBookieRecoveryDelay value (in seconds) in metadata store.
 */
@CustomLog
public class LostBookieRecoveryDelayCommand extends BookieCommand<LostBookieRecoveryDelayCommand.LBRDFlags> {


    private static final String NAME = "lostbookierecoverydelay";
    private static final String DESC =
        "Setter and Getter for LostBookieRecoveryDelay value (in seconds) in metadata store";

    private static final int DEFAULT = 0;

    public LostBookieRecoveryDelayCommand() {
        this(new LBRDFlags());
    }

    private LostBookieRecoveryDelayCommand(LBRDFlags flags) {
        super(CliSpec.<LostBookieRecoveryDelayCommand.LBRDFlags>newBuilder()
                  .withName(NAME)
                  .withDescription(DESC)
                  .withFlags(flags)
                  .build());
    }

    /**
     * Flags for command LostBookieRecoveryDelay.
     */
    @Accessors(fluent = true)
    @Setter
    public static class LBRDFlags extends CliFlags{

        @Parameter(names = { "-g", "--get" }, description = "Get LostBookieRecoveryDelay value (in seconds)")
        private boolean get;

        @Parameter(names = { "-s", "--set" }, description = "Set LostBookieRecoveryDelay value (in seconds)")
        private int set = DEFAULT;

    }

    @Override
    public boolean apply(ServerConfiguration conf, LBRDFlags cmdFlags) {
        try {
            return handler(conf, cmdFlags);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    public boolean handler(ServerConfiguration conf, LBRDFlags flags)
        throws InterruptedException, BKException, IOException, ReplicationException.UnavailableException,
               ReplicationException.CompatibilityException, KeeperException {
        boolean getter = flags.get;
        boolean setter = false;
        if (flags.set != DEFAULT) {
            setter = true;
        }

        if ((!getter && !setter) || (getter && setter)) {
            log.error("One and only one of -get and -set must be specified");
            return false;
        }
        ClientConfiguration adminConf = new ClientConfiguration(conf);
        BookKeeperAdmin admin = new BookKeeperAdmin(adminConf);
        try {
            if (getter) {
                int lostBookieRecoveryDelay = admin.getLostBookieRecoveryDelay();
                log.info().attr("delay", lostBookieRecoveryDelay).log("LostBookieRecoveryDelay value in ZK");
            } else {
                int lostBookieRecoveryDelay = flags.set;
                admin.setLostBookieRecoveryDelay(lostBookieRecoveryDelay);
                log.info().attr("delay", lostBookieRecoveryDelay)
                        .log("Successfully set LostBookieRecoveryDelay value in ZK");
            }
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
        return true;
    }
}
