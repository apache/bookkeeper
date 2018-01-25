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
package org.apache.distributedlog.stream.cli.commands.stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.distributedlog.clients.admin.StorageAdminClient;
import org.apache.distributedlog.stream.cli.commands.AdminCommand;
import org.apache.distributedlog.stream.proto.StreamProperties;

/**
 * Command to create a namespace.
 */
@Parameters(commandDescription = "Create a stream")
public class CreateStreamCommand extends AdminCommand {

    @Parameter(names = { "-n", "--namespace" }, description = "namespace name")
    private String namespaceName;
    @Parameter(names = { "-s", "--stream" }, description = "stream name")
    private String streamName;

    @Override
    protected void run(String defaultNamespace, StorageAdminClient admin) throws Exception {
        checkNotNull(streamName, "Stream name is not provided");
        System.out.println("Creating stream '" + streamName + "' ...");
        StreamProperties nsProps = result(
            admin.createStream(
                null == namespaceName ? defaultNamespace : namespaceName,
                streamName,
                DEFAULT_STREAM_CONF));
        System.out.println("Successfully created stream '" + streamName + "':");
        System.out.println(nsProps);
    }

    @Override
    public String name() {
        return "create";
    }
}
