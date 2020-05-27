/**
 *
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
 *
 */

package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.util.function.Supplier;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a read only bookie.
 * <p>
 * ReadOnlyBookie is force started as readonly, and will not change to writable.
 * </p>
 */
public class ReadOnlyBookie extends Bookie {

    private static final Logger LOG = LoggerFactory.getLogger(ReadOnlyBookie.class);

    public ReadOnlyBookie(ServerConfiguration conf, StatsLogger statsLogger,
            ByteBufAllocator allocator, Supplier<BookieServiceInfo> bookieServiceInfoProvider)
            throws IOException, KeeperException, InterruptedException, BookieException {
        super(conf, statsLogger, allocator, bookieServiceInfoProvider);
        if (conf.isReadOnlyModeEnabled()) {
            stateManager.forceToReadOnly();
        } else {
            String err = "Try to init ReadOnly Bookie, while ReadOnly mode is not enabled";
            LOG.error(err);
            throw new IOException(err);
        }
        LOG.info("Running bookie in force readonly mode.");
    }

    @Override
    StateManager initializeStateManager() throws IOException {
        return new BookieStateManager(conf, statsLogger, metadataDriver, getLedgerDirsManager(),
                                      bookieServiceInfoProvider) {

            @Override
            public void doTransitionToWritableMode() {
                // no-op
                LOG.info("Skip transition to writable mode for readonly bookie");
            }

            @Override
            public void doTransitionToReadOnlyMode() {
                // no-op
                LOG.info("Skip transition to readonly mode for readonly bookie");
            }
        };
    }
}
