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
package org.apache.bookkeeper.metadata.etcd;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.ZooKeeper;

/**
 * Etcd based registration client.
 */
class EtcdRegistrationClient implements RegistrationClient {
    @Override
    public RegistrationClient initialize(ClientConfiguration conf, ScheduledExecutorService scheduler, StatsLogger statsLogger, Optional<ZooKeeper> zkOptional) throws BKException {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public CompletableFuture<Versioned<Set<BookieSocketAddress>>> getWritableBookies() {
        return null;
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieSocketAddress>>> getReadOnlyBookies() {
        return null;
    }

    @Override
    public CompletableFuture<Void> watchWritableBookies(RegistrationListener listener) {
        return null;
    }

    @Override
    public void unwatchWritableBookies(RegistrationListener listener) {

    }

    @Override
    public CompletableFuture<Void> watchReadOnlyBookies(RegistrationListener listener) {
        return null;
    }

    @Override
    public void unwatchReadOnlyBookies(RegistrationListener listener) {

    }

    @Override
    public LayoutManager getLayoutManager() {
        return null;
    }
}
