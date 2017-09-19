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

package org.apache.bookkeeper.server.service;

import java.io.IOException;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.replication.AutoRecoveryMain;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * A {@link org.apache.bookkeeper.common.component.LifecycleComponent} that runs autorecovery.
 */
public class AutoRecoveryService extends AbstractLifecycleComponent<BookieConfiguration> {

    public static final String NAME = "autorecovery";

    private final AutoRecoveryMain main;

    public AutoRecoveryService(BookieConfiguration conf, StatsLogger statsLogger) throws Exception {
        super(NAME, conf, statsLogger);
        this.main = new AutoRecoveryMain(
            conf.getServerConf(),
            statsLogger);
    }

    @Override
    protected void doStart() {
        try {
            this.main.start();
        } catch (UnavailableException e) {
            throw new RuntimeException("Can't not start '" + NAME + "' component.", e);
        }
    }

    @Override
    protected void doStop() {
        // no-op
    }

    @Override
    protected void doClose() throws IOException {
        this.main.shutdown();
    }
}
