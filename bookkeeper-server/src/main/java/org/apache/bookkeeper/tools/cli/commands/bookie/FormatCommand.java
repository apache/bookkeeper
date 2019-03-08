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

package org.apache.bookkeeper.tools.cli.commands.bookie;

import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithRegistrationManager;

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.concurrent.ExecutionException;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to format the current server contents.
 */
public class FormatCommand extends BookieCommand<FormatCommand.Flags> {

    static final Logger LOG = LoggerFactory.getLogger(FormatCommand.class);

    private static final String NAME = "format";
    private static final String DESC = "Format the current server contents.";

    public FormatCommand() {
        this(new Flags());
    }

    public FormatCommand(Flags flags) {
        super(CliSpec.<Flags>newBuilder()
            .withName(NAME)
            .withDescription(DESC)
            .withFlags(flags)
            .build());
    }

    /**
     * Flags for format bookie command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class Flags extends CliFlags {

        @Parameter(names = {"-n", "--noninteractive"},
            description = "Whether to confirm if old data exists?")
        private boolean nonInteractive;

        @Parameter(names = {"-f", "--force"},
            description = "If [noninteractive] is specified, then whether"
                + "to force delete the old data without prompt?")
        private boolean force;

        @Parameter(names = {"-d", "--deletecookie"},
            description = "Delete its cookie on metadata store.")
        private boolean deleteCookie;

    }

    @Override
    public boolean apply(ServerConfiguration conf, Flags cmdFlags) {

        ServerConfiguration bfconf = new ServerConfiguration(conf);
        boolean result = Bookie.format(bfconf, cmdFlags.nonInteractive, cmdFlags.force);

        // delete cookie
        if (cmdFlags.deleteCookie) {
            try {
                runFunctionWithRegistrationManager(conf, rm -> {

                    try {
                        Versioned<Cookie> cookie = Cookie.readFromRegistrationManager(rm, bfconf);
                        cookie.getValue().deleteFromRegistrationManager(rm, bfconf, cookie.getVersion());
                    } catch (Exception e) {
                        throw new UncheckedExecutionException(e.getMessage(), e);
                    }

                    return null;
                });
            } catch (MetadataException | ExecutionException e) {
                throw new UncheckedExecutionException(e.getMessage(), e);
            }
        }
        return result;
    }
}
