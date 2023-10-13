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

package org.apache.bookkeeper.tools.cli.commands.cookie;

import com.beust.jcommander.Parameter;
import java.io.File;
import java.io.PrintStream;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.bookie.Cookie.Builder;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.tools.cli.commands.cookie.GenerateCookieCommand.Flags;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.commons.lang3.StringUtils;

/**
 * A command that generate cookie.
 */
@Slf4j
public class GenerateCookieCommand extends CookieCommand<Flags> {

    private static final String NAME = "generate";
    private static final String DESC = "Generate a cookie for a given bookie";

    private static final String USAGE = "cookie_generate      Generate a cookie for a given bookie\n"
            + "             Usage:  cookie_generate [options]\n"
            + "             Options:\n"
            + "               -i, --instance-id\n"
            + "              The instance id of the cluster that this bookie belongs to."
            + " If omitted, it will used the instance id of the cluster that this cli connects to. \n"
            + "             * -j, --journal-dirs\n"
            + "              The journal directories used by this bookie "
                                + "(param format: `journalDir1,...,journalDirM`)\n"
            + "             * -l, --ledger-dirs\n"
            + "              The ledger directories used by this bookie (param format: `ledgerDir1,...,ledgerDirN`)\n"
            + "             * -o, --output-file\n"
            + "              The output file to save the generated cookie (param format: `cookieLocalFilePath`)\n"
            + "               -ix, --index-dirs\n"
            + "              The index directories used by this bookie (param format: `indexDir1,...,indexDirN`)";

    /**
     * Flags to generate a cookie for a given bookie.
     */
    @Accessors(fluent = true)
    @Setter
    public static class Flags extends CliFlags {

        @Parameter(
            names = { "-j", "--journal-dirs" },
            description = "The journal directories used by this bookie",
            required = true)
        private String journalDirs;

        @Parameter(
            names = { "-l", "--ledger-dirs" },
            description = "The ledger directories used by this bookie",
            required = true)
        private String ledgerDirs;

        @Parameter(
                names = { "-ix", "--index-dirs" },
                description = "The index directories used by this bookie")
        private String indexDirs = null;

        @Parameter(
            names = { "-i", "--instance-id" },
            description = "The instance id of the cluster that this bookie belongs to."
                + " If omitted, it will used the instance id of the cluster that this cli connects to.")
        private String instanceId = null;

        @Parameter(
            names = { "-o", "--output-file" },
            description = "The output file to save the generated cookie.",
            required = true)
        private String outputFile;

    }

    public GenerateCookieCommand() {
        this(new Flags());
    }

    GenerateCookieCommand(PrintStream console) {
        this(new Flags(), console);
    }

    public GenerateCookieCommand(Flags flags) {
        this(flags, System.out);
    }

    private GenerateCookieCommand(Flags flags, PrintStream console) {
        super(CliSpec.<Flags>newBuilder()
            .withName(NAME)
            .withUsage(USAGE)
            .withDescription(DESC)
            .withFlags(flags)
            .withConsole(console)
            .withArgumentsUsage("<bookie-id>")
            .build());
    }

    @Override
    protected void apply(RegistrationManager rm, Flags cmdFlags) throws Exception {
        BookieId bookieId = getBookieId(cmdFlags);

        String instanceId;
        if (null == cmdFlags.instanceId) {
            instanceId = rm.getClusterInstanceId();
        } else {
            instanceId = cmdFlags.instanceId;
        }

        Builder builder = Cookie.newBuilder();
        builder.setBookieId(bookieId.toString());
        if (StringUtils.isEmpty(instanceId)) {
            builder.setInstanceId(null);
        } else {
            builder.setInstanceId(instanceId);
        }
        builder.setJournalDirs(cmdFlags.journalDirs);
        builder.setLedgerDirs(Cookie.encodeDirPaths(cmdFlags.ledgerDirs.split(",")));
        if (StringUtils.isNotBlank(cmdFlags.indexDirs)) {
            builder.setIndexDirs(Cookie.encodeDirPaths(cmdFlags.indexDirs.split(",")));
        }
        Cookie cookie = builder.build();
        cookie.writeToFile(new File(cmdFlags.outputFile));
        spec.console().println("Successfully saved the generated cookie to " + cmdFlags.outputFile);
    }

}
