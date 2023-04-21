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

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.admin.StorageAdminClient;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.commons.configuration.CompositeConfiguration;

/**
 * An admin command interface provides a run method to execute admin commands.
 */
@Slf4j
public abstract class AdminCommand<ClientFlagsT extends CliFlags> extends AbstractStreamCommand<ClientFlagsT> {

    protected AdminCommand(CliSpec<ClientFlagsT> spec) {
        super(spec);
    }

    @Override
    protected boolean doApply(ServiceURI serviceURI,
                              CompositeConfiguration conf,
                              BKFlags bkFlags,
                              ClientFlagsT cmdFlags) {
        StorageClientSettings settings = StorageClientSettings.newBuilder()
            .clientName("bkctl")
            .serviceUri(serviceURI.getUri().toString())
            .build();

        try (StorageAdminClient admin = StorageClientBuilder.newBuilder()
            .withSettings(settings)
            .buildAdmin()) {
            run(admin, bkFlags, cmdFlags);
            return true;
        } catch (Exception e) {
            log.error("Failed to process stream admin command", e);
            spec.console().println("Failed to process stream admin command");
            e.printStackTrace(spec.console());
            return false;
        }
    }

    protected abstract void run(StorageAdminClient admin, BKFlags globalFlags, ClientFlagsT cmdFlags)
        throws Exception;
}
