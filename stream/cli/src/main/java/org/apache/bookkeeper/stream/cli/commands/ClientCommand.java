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
package org.apache.bookkeeper.stream.cli.commands;

import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.config.StorageClientSettings;

/**
 * An admin command interface provides a run method to execute admin commands.
 */
public abstract class ClientCommand implements SubCommand {

    @Override
    public void run(String namespace, StorageClientSettings settings) throws Exception {
        try (StorageClient client = StorageClientBuilder.newBuilder()
            .withSettings(settings)
            .withNamespace(namespace)
            .build()) {
            run(client);
        }
    }

    protected abstract void run(StorageClient client) throws Exception;
}
