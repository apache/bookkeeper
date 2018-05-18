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

package org.apache.bookkeeper.tests.integration.topologies;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.tests.containers.BookieContainer;
import org.apache.bookkeeper.tests.containers.MetadataStoreContainer;
import org.apache.bookkeeper.tests.containers.ZKContainer;
import org.testcontainers.containers.Network;

/**
 * BookKeeper Cluster in containers.
 */
@Slf4j
public class BKCluster {

    @Getter
    private final String clusterName;
    private final Network network;
    private final MetadataStoreContainer metadataContainer;
    private final Map<String, BookieContainer> bookieContainers;
    private final int numBookies;

    public BKCluster(String clusterName, int numBookies) {
        this.clusterName = clusterName;
        this.network = Network.newNetwork();
        this.metadataContainer = (MetadataStoreContainer) new ZKContainer(clusterName)
            .withNetwork(network)
            .withNetworkAliases(ZKContainer.HOST_NAME);
        this.bookieContainers = Maps.newTreeMap();
        this.numBookies = numBookies;
    }

    public String getExternalServiceUri() {
        return metadataContainer.getExternalServiceUri();
    }

    public String getInternalServiceUri() {
        return metadataContainer.getInternalServiceUri();
    }

    public void start() throws Exception {
        // start the metadata store
        this.metadataContainer.start();

        // init a new cluster
        initNewCluster(metadataContainer.getExternalServiceUri());

        // create bookies
        createBookies("bookie", numBookies);
    }

    public void stop() {
        synchronized (this) {
            bookieContainers.values().forEach(BookieContainer::stop);
        }

        this.metadataContainer.stop();
        try {
            this.network.close();
        } catch (Exception e) {
            log.info("Failed to shutdown network for bookkeeper cluster {}", clusterName, e);
        }
    }

    protected void initNewCluster(String metadataServiceUri) throws Exception {
        MetadataDrivers.runFunctionWithRegistrationManager(
            new ServerConfiguration().setMetadataServiceUri(metadataServiceUri),
            rm -> {
                try {
                    rm.initNewCluster();
                } catch (Exception e) {
                    throw new UncheckedExecutionException("Failed to init a new cluster", e);
                }
                return null;
            }
        );
    }

    public synchronized Map<String, BookieContainer> getBookieContainers() {
        return bookieContainers;
    }


    public synchronized BookieContainer getBookie(String bookieName) {
        return bookieContainers.get(bookieName);
    }

    public BookieContainer killBookie(String bookieName) {
        BookieContainer container;
        synchronized (this) {
            container = bookieContainers.remove(bookieName);
            if (null != container) {
                container.stop();
            }
        }
        return container;
    }

    public synchronized BookieContainer createBookie(String bookieName) {
        BookieContainer container = getBookie(bookieName);
        if (null == container) {
            container = (BookieContainer) new BookieContainer(clusterName, bookieName, ZKContainer.SERVICE_URI)
                .withNetwork(network)
                .withNetworkAliases(bookieName);
            container.start();
            bookieContainers.put(bookieName, container);
        }
        return container;
    }

    public synchronized Map<String, BookieContainer> createBookies(String bookieNamePrefix, int numBookies)
            throws Exception {
        List<CompletableFuture<Void>> startFutures = Lists.newArrayListWithExpectedSize(numBookies);
        Map<String, BookieContainer> containers = Maps.newHashMap();
        for (int i = 0; i < numBookies; i++) {
            final int idx = i;
            startFutures.add(
                CompletableFuture.runAsync(() -> {
                    String bookieName = String.format("%s-%03d", bookieNamePrefix, idx);
                    BookieContainer container = createBookie(bookieName);
                    synchronized (containers) {
                        containers.put(bookieName, container);
                    }
                }));
        }
        FutureUtils.result(FutureUtils.collect(startFutures));
        return containers;
    }
}
