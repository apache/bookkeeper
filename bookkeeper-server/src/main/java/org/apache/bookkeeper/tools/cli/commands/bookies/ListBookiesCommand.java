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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.bookkeeper.tools.cli.helpers.CommandHelpers.getBookieSocketAddrStringRepresentation;

import com.beust.jcommander.Parameter;
import java.util.Collection;
import java.util.Set;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.client.DefaultBookieAddressResolver;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.tools.cli.commands.bookies.ListBookiesCommand.Flags;
import org.apache.bookkeeper.tools.cli.helpers.DiscoveryCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to list available bookies.
 */
public class ListBookiesCommand extends DiscoveryCommand<Flags> {

    private static final String NAME = "list";
    private static final String DESC = "List the bookies, which are running as either readwrite or readonly mode.";
    private static final Logger LOG = LoggerFactory.getLogger(ListBookiesCommand.class);

    public ListBookiesCommand() {
        this(new Flags());
    }

    public ListBookiesCommand(Flags flags) {
        super(CliSpec.<Flags>newBuilder()
            .withName(NAME)
            .withDescription(DESC)
            .withFlags(flags)
            .build());
    }

    /**
     * Flags for list bookies command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class Flags extends CliFlags {

        @Parameter(names = { "-rw", "--readwrite" }, description = "Print readwrite bookies")
        private boolean readwrite = false;
        @Parameter(names = { "-ro", "--readonly" }, description = "Print readonly bookies")
        private boolean readonly = false;
        @Parameter(names = { "-a", "--all" }, description = "Print all bookies")
        private boolean all = false;

    }

    @Override
    protected void run(RegistrationClient regClient, Flags flags) throws Exception {
        if (!flags.readwrite && !flags.readonly && !flags.all) {
            // case: no args is provided. list all the bookies by default.
            flags.readwrite = true;
            flags.readonly = true;
            flags.all = true;
        }

        boolean hasBookies = false;
        if (flags.readwrite) {
            Set<BookieId> bookies = result(
                regClient.getWritableBookies()
            ).getValue();
            if (!bookies.isEmpty()) {
                LOG.info("ReadWrite Bookies :");
                printBookies(bookies, new DefaultBookieAddressResolver(regClient));
                hasBookies = true;
            }
        }
        if (flags.readonly) {
            Set<BookieId> bookies = result(
                regClient.getReadOnlyBookies()
            ).getValue();
            if (!bookies.isEmpty()) {
                LOG.info("Readonly Bookies :");
                printBookies(bookies, new DefaultBookieAddressResolver(regClient));
                hasBookies = true;
            }
        }
        if (flags.all) {
            Set<BookieId> bookies = result(
                regClient.getAllBookies()
            ).getValue();
            if (!bookies.isEmpty()) {
                LOG.info("All Bookies :");
                printBookies(bookies, new DefaultBookieAddressResolver(regClient));
                hasBookies = true;
            }
        }
        if (!hasBookies) {
            LOG.error("No bookie exists!");
        }
    }

    private static void printBookies(Collection<BookieId> bookies, BookieAddressResolver bookieAddressResolver) {
        for (BookieId b : bookies) {
            LOG.info(getBookieSocketAddrStringRepresentation(b, bookieAddressResolver));
        }
    }

}
