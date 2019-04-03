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
package org.apache.bookkeeper.tools.cli.commands.bookies;

import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithRegistrationManager;

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to trigger AuditTask by resetting lostBookieRecoveryDelay and
 * then make sure the ledgers stored in the bookie are properly replicated
 * and Cookie of the decommissioned bookie should be deleted from metadata
 * server.
 */
public class DecommissionCommand extends BookieCommand<DecommissionCommand.DecommissionFlags> {

    static final Logger LOG = LoggerFactory.getLogger(DecommissionCommand.class);

    private static final String NAME = "decommission";
    private static final String DESC =
        "Force trigger the Audittask and make sure all the ledgers stored in the decommissioning bookie"
        + " are replicated and cookie of the decommissioned bookie is deleted from metadata server.";

    public DecommissionCommand() {
        this(new DecommissionFlags());
    }

    private DecommissionCommand(DecommissionFlags flags) {
        super(CliSpec.<DecommissionFlags>newBuilder().withName(NAME).withDescription(DESC).withFlags(flags).build());
    }

    /**
     * Flags for decommission command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class DecommissionFlags extends CliFlags {

        @Parameter(names = { "-b", "--bookieid" }, description = "Decommission a remote bookie")
        private String remoteBookieIdToDecommission;

    }

    @Override
    public boolean apply(ServerConfiguration conf, DecommissionFlags cmdFlags) {
        try {
            return decommission(conf, cmdFlags);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private boolean decommission(ServerConfiguration conf, DecommissionFlags flags)
        throws BKException, InterruptedException, IOException {
        ClientConfiguration adminConf = new ClientConfiguration(conf);
        BookKeeperAdmin admin = new BookKeeperAdmin(adminConf);
        try {
            final String remoteBookieidToDecommission = flags.remoteBookieIdToDecommission;
            final BookieSocketAddress bookieAddressToDecommission = (StringUtils.isBlank(remoteBookieidToDecommission)
                                                                         ? Bookie.getBookieAddress(conf)
                                                                         : new BookieSocketAddress(
                                                                             remoteBookieidToDecommission));
            admin.decommissionBookie(bookieAddressToDecommission);
            LOG.info("The ledgers stored in the given decommissioning bookie: {} are properly replicated",
                     bookieAddressToDecommission);
            runFunctionWithRegistrationManager(conf, rm -> {
                try {
                    Versioned<Cookie> cookie = Cookie.readFromRegistrationManager(rm, bookieAddressToDecommission);
                    cookie.getValue().deleteFromRegistrationManager(rm, bookieAddressToDecommission,
                                                                    cookie.getVersion());
                } catch (BookieException.CookieNotFoundException nne) {
                    LOG.warn("No cookie to remove for the decommissioning bookie: {}, it could be deleted already",
                             bookieAddressToDecommission, nne);
                } catch (BookieException be) {
                    throw new UncheckedExecutionException(be.getMessage(), be);
                }
                return true;
            });
            LOG.info("Cookie of the decommissioned bookie: {} is deleted successfully",
                     bookieAddressToDecommission);
            return true;
        } catch (Exception e) {
            LOG.error("Received exception in DecommissionBookieCmd ", e);
            return false;
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }
}
