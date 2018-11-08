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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.tools.cli.helpers.BookieShellCommand;
import org.apache.bookkeeper.tools.common.BKCommand;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.commons.configuration.CompositeConfiguration;

/**
 * This is a mixin for cookie related commands to extends.
 */
@Slf4j
abstract class CookieCommand<CookieFlagsT extends CliFlags>
    extends BKCommand<CookieFlagsT> {

    protected CookieCommand(CliSpec<CookieFlagsT> spec) {
        super(spec);
    }

    @Override
    protected boolean apply(ServiceURI serviceURI,
                            CompositeConfiguration conf,
                            BKFlags globalFlags,
                            CookieFlagsT cmdFlags) {
        ServerConfiguration serverConf = new ServerConfiguration();
        serverConf.loadConf(conf);

        if (null != serviceURI) {
            serverConf.setMetadataServiceUri(serviceURI.getUri().toString());
        }

        try {
            return MetadataDrivers.runFunctionWithRegistrationManager(serverConf, registrationManager -> {
                try {
                    apply(registrationManager, cmdFlags);
                    return true;
                } catch (Exception e) {
                    throw new UncheckedExecutionException(e);
                }
            });
        } catch (MetadataException | ExecutionException | UncheckedExecutionException e) {
            Throwable cause = e;
            if (!(e instanceof MetadataException) && null != e.getCause()) {
                cause = e.getCause();
            }
            spec.console().println("Failed to process cookie command '" + name() + "'");
            cause.printStackTrace(spec.console());
            return false;
        }
    }

    protected String getBookieId(CookieFlagsT cmdFlags) throws UnknownHostException {
        checkArgument(
            cmdFlags.arguments.size() == 1,
            "No bookie id or more bookie ids is specified");

        String bookieId = cmdFlags.arguments.get(0);
        try {
            new BookieSocketAddress(bookieId);
        } catch (UnknownHostException nhe) {
            spec.console()
                .println("Invalid bookie id '"
                    + bookieId + "'is used to create cookie."
                    + " Bookie id should be in the format of '<hostname>:<port>'");
            throw nhe;
        }
        return bookieId;
    }

    protected byte[] readCookieDataFromFile(String cookieFile) throws IOException {
        try {
            return Files.readAllBytes(Paths.get(cookieFile));
        } catch (NoSuchFileException nfe) {
            spec.console()
                .println("Cookie file '" + cookieFile + "' doesn't exist.");
            throw nfe;
        }
    }


    protected abstract void apply(RegistrationManager rm, CookieFlagsT cmdFlags)
        throws Exception;

    public org.apache.bookkeeper.bookie.BookieShell.Command asShellCommand(String shellCmdName,
                                                                           CompositeConfiguration conf) {
        return new BookieShellCommand<>(shellCmdName, this, conf);
    }
}
