/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.stream.cli.commands.namespace;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.bookkeeper.clients.admin.StorageAdminClient;
import org.apache.bookkeeper.stream.cli.commands.AdminCommand;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.NamespaceProperties;

/**
 * Command to create a namespace.
 */
@Parameters(commandDescription = "Create a namespace")
public class CreateNamespaceCommand extends AdminCommand {

    @Parameter(names = { "-n", "--name" }, description = "namespace name")
    private String namespaceName;

    @Override
    protected void run(String namespace, StorageAdminClient admin) throws Exception {
        checkNotNull(namespaceName, "Namespace name is not provided");
        System.out.println("Creating namespace '" + namespaceName + "' ...");
        NamespaceProperties nsProps = result(
            admin.createNamespace(
                namespaceName,
                NamespaceConfiguration.newBuilder()
                    .setDefaultStreamConf(DEFAULT_STREAM_CONF)
                    .build()));
        System.out.println("Successfully created namespace '" + namespaceName + "':");
        System.out.println(nsProps);
    }

    @Override
    public String name() {
        return "create";
    }
}
