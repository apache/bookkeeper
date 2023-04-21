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


import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to print which node has the auditor lock.
 */
public class WhoIsAuditorCommand extends BookieCommand<CliFlags> {

    static final Logger LOG = LoggerFactory.getLogger(WhoIsAuditorCommand.class);

    private static final String NAME = "whoisauditor";
    private static final String DESC = "Print the node which holds the auditor lock.";

    private BookKeeperAdmin bka;

    public WhoIsAuditorCommand() {
        this(null);
    }

    public WhoIsAuditorCommand(BookKeeperAdmin bka) {
        super(CliSpec.newBuilder()
                     .withName(NAME)
                     .withDescription(DESC)
                     .withFlags(new CliFlags())
                     .build());
        this.bka = bka;
    }

    @Override
    public boolean apply(ServerConfiguration conf, CliFlags cmdFlags) {
        try {
            return getAuditor(conf);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private boolean getAuditor(ServerConfiguration conf)
            throws BKException, InterruptedException, IOException {
        ClientConfiguration clientConfiguration = new ClientConfiguration(conf);

        BookieId bookieId;
        if (this.bka != null) {
            bookieId = bka.getCurrentAuditor();
        } else {
            @Cleanup
            BookKeeperAdmin bka = new BookKeeperAdmin(clientConfiguration);
            bookieId = bka.getCurrentAuditor();
        }
        if (bookieId == null) {
            LOG.info("No auditor elected");
            return false;
        }
        LOG.info("Auditor: " + bookieId);
        return true;
    }
}
