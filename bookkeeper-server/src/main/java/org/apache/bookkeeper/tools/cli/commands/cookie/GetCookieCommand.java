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

import java.io.PrintStream;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieException.CookieNotFoundException;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.tools.cli.commands.cookie.GetCookieCommand.Flags;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * A command that deletes cookie.
 */
@Slf4j
public class GetCookieCommand extends CookieCommand<Flags> {

    private static final String NAME = "get";
    private static final String DESC = "Retrieve a cookie for a given bookie";

    /**
     * Flags to delete a cookie for a given bookie.
     */
    @Accessors(fluent = true)
    @Setter
    public static class Flags extends CliFlags {
    }

    public GetCookieCommand() {
        this(new Flags());
    }

    GetCookieCommand(PrintStream console) {
        this(new Flags(), console);
    }

    public GetCookieCommand(Flags flags) {
        this(flags, System.out);
    }

    private GetCookieCommand(Flags flags, PrintStream console) {
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

        try {
            Versioned<Cookie> cookie = Cookie.readFromRegistrationManager(
                rm, new BookieSocketAddress(bookieId)
            );
            spec.console().println("Cookie for bookie '" + bookieId + "' is:");
            spec.console().println("---");
            spec.console().println(
                cookie.getValue()
            );
            spec.console().println("---");
        } catch (CookieNotFoundException cee) {
            spec.console()
                .println("Cookie not found for bookie '" + bookieId + "'");
            throw cee;
        } catch (BookieException be) {
            spec.console()
                .println("Exception on getting cookie for bookie '" + bookieId + "'");
            be.printStackTrace(spec.console());
            throw be;
        }
    }

}
