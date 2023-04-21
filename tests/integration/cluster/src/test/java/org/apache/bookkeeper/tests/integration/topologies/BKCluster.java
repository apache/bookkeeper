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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.stream.storage.impl.cluster.ZkClusterInitializer;
import org.apache.bookkeeper.tests.containers.BookieContainer;
import org.apache.bookkeeper.tests.containers.MetadataStoreContainer;
import org.apache.bookkeeper.tests.containers.ZKContainer;
import org.testcontainers.containers.Network;

/**
 * BookKeeper Cluster in containers.
 */
@Slf4j
public class BKCluster {

    /**
     * BookKeeper Cluster Spec.
     *
     * @param spec bookkeeper cluster spec.
     * @return the built bookkeeper cluster
     */
    public static BKCluster forSpec(BKClusterSpec spec) {
        return new BKCluster(spec);
    }

    private final BKClusterSpec spec;
    @Getter
    private final String clusterName;
    private final Network network;
    private final MetadataStoreContainer metadataContainer;
    private final Map<String, BookieContainer> bookieContainers;
    private final Map<String, String> internalEndpointsToExternalEndpoints;
    private final int numBookies;
    private final String extraServerComponents;
    private volatile boolean enableContainerLog;

    private BKCluster(BKClusterSpec spec) {
        this.spec = spec;
        this.clusterName = spec.clusterName();
        this.network = Network.newNetwork();
        this.metadataContainer = (MetadataStoreContainer) new ZKContainer(clusterName)
            .withNetwork(network)
            .withNetworkAliases(ZKContainer.HOST_NAME);
        this.bookieContainers = Maps.newTreeMap();
        this.internalEndpointsToExternalEndpoints = Maps.newConcurrentMap();
        this.numBookies = spec.numBookies();
        this.extraServerComponents = spec.extraServerComponents();
        this.enableContainerLog = spec.enableContainerLog();
    }

    public String getExternalServiceUri() {
        return metadataContainer.getExternalServiceUri();
    }

    public String getInternalServiceUri() {
        return metadataContainer.getInternalServiceUri();
    }

    public void start() throws Exception {
        // start the metadata store
        if (enableContainerLog) {
            this.metadataContainer.tailContainerLog();
        }
        this.metadataContainer.start();
        log.info("Successfully started metadata store container.");

        // init a new cluster
        initNewCluster(metadataContainer.getExternalServiceUri());
        log.info("Successfully initialized metadata service uri : {}",
            metadataContainer.getExternalServiceUri());

        if (!Strings.isNullOrEmpty(extraServerComponents)) {
            int numStorageContainers = numBookies > 0 ? 2 * numBookies : 8;
            // initialize the stream storage.
            new ZkClusterInitializer(
                ZKMetadataDriverBase.getZKServersFromServiceUri(URI.create(metadataContainer.getExternalServiceUri()))
            ).initializeCluster(
                URI.create(metadataContainer.getInternalServiceUri()),
                numStorageContainers);
            log.info("Successfully initialized stream storage metadata with {} storage containers",
                numStorageContainers);
        }

        // create bookies
        createBookies("bookie", numBookies);
    }

    public String resolveExternalGrpcEndpointStr(String internalGrpcEndpointStr) {
        String externalGrpcEndpointStr = internalEndpointsToExternalEndpoints.get(internalGrpcEndpointStr);
        checkNotNull(externalGrpcEndpointStr,
            "No internal grpc endpoint is found : " + internalGrpcEndpointStr);
        return externalGrpcEndpointStr;
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

    public synchronized BookieContainer getAnyBookie() {
        List<BookieContainer> bookieList = Lists.newArrayList();
        bookieList.addAll(bookieContainers.values());
        Collections.shuffle(bookieList);
        checkArgument(!bookieList.isEmpty(), "No bookie is alive");
        return bookieList.get(0);
    }

    public BookieContainer killBookie(String bookieName) {
        BookieContainer container;
        synchronized (this) {
            container = bookieContainers.remove(bookieName);
            if (null != container) {
                internalEndpointsToExternalEndpoints.remove(container.getInternalGrpcEndpointStr());
                container.stop();
            }
        }
        return container;
    }

    public BookieContainer createBookie(String bookieName) {
        boolean shouldStart = false;
        BookieContainer container;
        synchronized (this) {
            container = getBookie(bookieName);
            if (null == container) {
                shouldStart = true;
                log.info("Creating bookie {}", bookieName);
                container = (BookieContainer) new BookieContainer(
                    clusterName, bookieName, ZKContainer.SERVICE_URI, extraServerComponents
                ).withNetwork(network).withNetworkAliases(bookieName);
                if (enableContainerLog) {
                    container.tailContainerLog();
                }
                bookieContainers.put(bookieName, container);
            }
        }

        if (shouldStart) {
            log.info("Starting bookie {}", bookieName);
            container.start();
            log.info("Started bookie {} : internal endpoint = {}, external endpoint = {}",
                bookieName, container.getInternalGrpcEndpointStr(), container.getExternalGrpcEndpointStr());
            internalEndpointsToExternalEndpoints.put(
                container.getInternalGrpcEndpointStr(),
                container.getExternalGrpcEndpointStr());
        }
        return container;
    }

    public Map<String, BookieContainer> createBookies(String bookieNamePrefix, int numBookies)
            throws Exception {
        log.info("Creating {} bookies with bookie name prefix '{}'", numBookies, bookieNamePrefix);
        List<CompletableFuture<Void>> startFutures = Lists.newArrayListWithExpectedSize(numBookies);
        Map<String, BookieContainer> containers = Maps.newHashMap();
        for (int i = 0; i < numBookies; i++) {
            final int idx = i;
            startFutures.add(
                CompletableFuture.runAsync(() -> {
                    String bookieName = String.format("%s-%03d", bookieNamePrefix, idx);
                    log.info("Starting bookie {} at cluster {}", bookieName, clusterName);
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
