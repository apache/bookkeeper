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
import java.io.PrintStream;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieException.CookieNotFoundException;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.tools.cli.commands.cookie.UpdateCookieCommand.Flags;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * A command that updates cookie.
 */
@Slf4j
public class UpdateCookieCommand extends CookieCommand<Flags> {

    private static final String NAME = "update";
    private static final String DESC = "Update a cookie for a given bookie";

    /**
     * Flags to create a cookie for a given bookie.
     */
    @Accessors(fluent = true)
    @Setter
    public static class Flags extends CliFlags {

        @Parameter(
            names = { "-cf", "--cookie-file" },
            description = "The file to be uploaded as cookie",
            required = true)
        private String cookieFile;

    }

    public UpdateCookieCommand() {
        this(new Flags());
    }

    UpdateCookieCommand(PrintStream console) {
        this(new Flags(), console);
    }

    public UpdateCookieCommand(Flags flags) {
        this(flags, System.out);
    }

    private UpdateCookieCommand(Flags flags, PrintStream console) {
        super(CliSpec.<Flags>newBuilder()
            .withName(NAME)
            .withDescription(DESC)
            .withFlags(flags)
            .withConsole(console)
            .withArgumentsUsage("<bookie-id>")
            .build());
    }

    @Override
    protected void apply(RegistrationManager rm, Flags cmdFlags) throws Exception {
        String bookieId = getBookieId(cmdFlags);

        byte[] data = readCookieDataFromFile(cmdFlags.cookieFile);
        Versioned<byte[]> cookie = new Versioned<>(data, new LongVersion(-1L));
        try {
            rm.writeCookie(bookieId, cookie);
        } catch (CookieNotFoundException cnfe) {
            spec.console()
                .println("Cookie not found for bookie '" + bookieId + "' to update");
            throw cnfe;
        } catch (BookieException be) {
            spec.console()
                .println("Exception on updating cookie for bookie '" + bookieId + "'");
            be.printStackTrace(spec.console());
            throw be;
        }
    }

}
