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
package org.apache.bookkeeper.tools.cli.commands.cluster;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.bookkeeper.tools.cli.helpers.CommandHelpers.getBookieSocketAddrStringRepresentation;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;
import java.util.List;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.tools.cli.helpers.DiscoveryCommand;

/**
 * Command to list available bookies.
 */
@Accessors(fluent = true)
@Setter
@Parameters(commandDescription = "List the bookies, which are running as either readwrite or readonly mode.")
public class ListBookiesCommand extends DiscoveryCommand {

    @Parameter(names = { "-rw", "--readwrite" }, description = "Print readwrite bookies")
    private boolean readwrite = false;
    @Parameter(names = { "-ro", "--readonly" }, description = "Print readonly bookies")
    private boolean readonly = false;

    @Override
    protected void run(RegistrationClient regClient) throws Exception {
        List<BookieSocketAddress> bookies = Lists.newArrayList();
        if (readwrite) {
            bookies.addAll(
                result(
                    regClient.getWritableBookies()
                ).getValue()
            );
        } else if (readonly) {
            bookies.addAll(
                result(
                    regClient.getReadOnlyBookies()
                ).getValue()
            );
        }
        for (BookieSocketAddress b : bookies) {
            System.out.println(getBookieSocketAddrStringRepresentation(b));
        }
        if (bookies.isEmpty()) {
            System.err.println("No bookie exists!");
        }
    }

    @Override
    public String name() {
        return "listbookies";
    }
}
