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
package org.apache.bookkeeper.tools.cli.helpers;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.ReflectionUtils;

/**
 * This is a mixin for commands that talks to discovery service.
 */
@Slf4j
public abstract class DiscoveryCommand extends ClientCommand {

    @Override
    protected void run(ClientConfiguration conf) throws Exception {
        Class<? extends RegistrationClient> regClientCls = conf.getRegistrationClientClass();
        @Cleanup("shutdown") ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try (RegistrationClient regClient = ReflectionUtils.newInstance(regClientCls)) {
            regClient.initialize(
                conf,
                executor,
                NullStatsLogger.INSTANCE,
                Optional.empty());
            run(regClient);
        }
    }

    @Override
    protected void run(BookKeeper bk) throws Exception {
        throw new UnsupportedOperationException("Discovery command only uses registration client");
    }

    protected abstract void run(RegistrationClient regClient) throws Exception;

}
