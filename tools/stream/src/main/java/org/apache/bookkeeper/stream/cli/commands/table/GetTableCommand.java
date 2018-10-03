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
package org.apache.bookkeeper.stream.cli.commands.table;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.bookkeeper.stream.cli.Commands.OP_GET;

import org.apache.bookkeeper.clients.admin.StorageAdminClient;
import org.apache.bookkeeper.clients.exceptions.NamespaceNotFoundException;
import org.apache.bookkeeper.clients.exceptions.StreamNotFoundException;
import org.apache.bookkeeper.stream.cli.commands.AdminCommand;
import org.apache.bookkeeper.stream.cli.commands.table.GetTableCommand.Flags;
import org.apache.bookkeeper.stream.proto.StorageType;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;

/**
 * Command to create a namespace.
 */
public class GetTableCommand extends AdminCommand<Flags> {

    private static final String NAME = OP_GET;
    private static final String DESC = "Get the details of a table";

    /**
     * Flags for the create table command.
     */
    public static class Flags extends CliFlags {
    }

    public GetTableCommand() {
        super(CliSpec.<Flags>newBuilder()
            .withName(NAME)
            .withDescription(DESC)
            .withFlags(new Flags())
            .withArgumentsUsage("<table-name>")
            .build());
    }

    @Override
    protected void run(StorageAdminClient admin,
                       BKFlags globalFlags,
                       Flags flags) throws Exception {
        checkArgument(!flags.arguments.isEmpty(),
            "Table name is not provided");

        String streamName = flags.arguments.get(0);

        try {
            StreamProperties streamProps = result(
                admin.getStream(
                    globalFlags.namespace,
                    streamName));
            if (streamProps.getStreamConf().getStorageType() != StorageType.TABLE) {
                spec.console().println("'" + streamName + "' is not a table storage entity, its storage type is "
                    + streamProps.getStreamConf().getStorageType());
            } else {
                spec.console().println("Table '" + streamName + "' :");
            }
            spec.console().println(streamProps);
        } catch (NamespaceNotFoundException nfe) {
            spec.console().println("Namespace '" + globalFlags.namespace + "' does not exist");
        } catch (StreamNotFoundException snfe) {
            spec.console().println("Table '" + streamName + "' does not exist");
        }
    }

}
