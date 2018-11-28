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

package org.apache.bookkeeper.grpc.resolver;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;

/**
 * A {@link NameResolver} implementation based on bookkeeper {@link org.apache.bookkeeper.discover.RegistrationClient}.
 */
class BKRegistrationNameResolver extends NameResolver {

    private final MetadataClientDriver clientDriver;
    private final URI serviceURI;
    private final ScheduledExecutorService executor;

    private Listener listener;
    private boolean shutdown;
    private boolean resolving;

    BKRegistrationNameResolver(MetadataClientDriver clientDriver,
                               URI serviceURI) {
        this.clientDriver = clientDriver;
        this.serviceURI = serviceURI;
        this.executor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("registration-name-resolver").build());
    }

    @Override
    public String getServiceAuthority() {
        return serviceURI.getAuthority();
    }

    @Override
    public synchronized void start(Listener listener) {
        checkState(null == this.listener, "Resolver already started");
        this.listener = Objects.requireNonNull(listener, "Listener is null");

        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(serviceURI.toString());

        try {
            clientDriver.initialize(conf, executor, NullStatsLogger.INSTANCE, Optional.empty());
        } catch (MetadataException e) {
            throw new RuntimeException("Failed to initialize registration client driver at " + serviceURI, e);
        }

        resolve();
    }

    private synchronized void resolve() {
        if (resolving || shutdown) {
            return;
        }
        resolving = true;
        this.clientDriver.getRegistrationClient().watchWritableBookies(bookies -> {
            Listener savedListener;
            synchronized (this) {
                savedListener = listener;
            }
            savedListener.onAddresses(
                hostsToEquivalentAddressGroups(bookies.getValue()),
                Attributes.EMPTY
            );
        }).whenComplete((ignored, cause) -> {
            try {
                if (null != cause) {
                    resolve();
                }
            } finally {
                synchronized (this) {
                    resolving = false;
                }
            }
        });
    }

    private static List<EquivalentAddressGroup> hostsToEquivalentAddressGroups(Set<BookieSocketAddress> bookies) {
        return bookies.stream()
            .map(addr -> new EquivalentAddressGroup(
                Collections.singletonList(addr.getSocketAddress()),
                Attributes.EMPTY
            ))
            .collect(Collectors.toList());
    }

    @Override
    public void shutdown() {
        synchronized (this) {
            if (shutdown) {
                return;
            }
            shutdown = true;
        }
        executor.shutdown();
        clientDriver.close();
    }
}
