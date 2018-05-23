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

package org.apache.bookkeeper.stream.server;

import java.net.UnknownHostException;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.server.component.ServerLifecycleComponent;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.server.conf.StorageServerConfiguration;
import org.apache.commons.configuration.ConfigurationException;

/**
 * This is a {@link ServerLifecycleComponent} to allow run stream storage component as part of bookie server.
 */
public class StreamStorageLifecycleComponent extends ServerLifecycleComponent {

    private final LifecycleComponent streamStorage;

    public StreamStorageLifecycleComponent(BookieConfiguration conf, StatsLogger statsLogger)
            throws UnknownHostException, ConfigurationException {
        super("stream-storage", conf, statsLogger);

        StorageServerConfiguration ssConf = StorageServerConfiguration.of(conf.getUnderlyingConf());

        this.streamStorage = StorageServer.buildStorageServer(
            conf.getUnderlyingConf(),
            ssConf.getGrpcPort(),
            false,
            statsLogger.scope("stream"));
    }

    @Override
    protected void doStart() {
        this.streamStorage.start();
    }

    @Override
    protected void doStop() {
        this.streamStorage.stop();
    }

    @Override
    protected void doClose() {
        this.streamStorage.close();
    }
}
