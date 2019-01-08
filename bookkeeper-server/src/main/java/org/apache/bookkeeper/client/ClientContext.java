/*
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
package org.apache.bookkeeper.client;

import io.netty.buffer.ByteBufAllocator;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookieClient;

/**
 * Collection of client objects used by LedgerHandle to interact with
 * the outside world. Normally these are instantiated by the BookKeeper object
 * but they are present to the LedgerHandle through this interface to allow
 * tests to easily inject mocked versions.
 */
interface ClientContext {
    ClientInternalConf getConf();
    LedgerManager getLedgerManager();
    BookieWatcher getBookieWatcher();
    EnsemblePlacementPolicy getPlacementPolicy();
    BookieClient getBookieClient();
    ByteBufAllocator getByteBufAllocator();
    OrderedExecutor getMainWorkerPool();
    OrderedScheduler getScheduler();
    BookKeeperClientStats getClientStats();
    boolean isClientClosed();
}
