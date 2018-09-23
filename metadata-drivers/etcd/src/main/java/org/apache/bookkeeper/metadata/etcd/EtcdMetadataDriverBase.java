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
package org.apache.bookkeeper.metadata.etcd;

import com.beust.jcommander.internal.Lists;
import com.coreos.jetcd.Client;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.exceptions.Code;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.configuration.ConfigurationException;

/**
 * This is a mixin class for supporting etcd based metadata drivers.
 */
@Slf4j
class EtcdMetadataDriverBase implements AutoCloseable {

    static final String SCHEME = "etcd";

    protected AbstractConfiguration<?> conf;
    protected StatsLogger statsLogger;

    // service uri
    protected Client client;
    protected String keyPrefix;

    // managers
    protected LayoutManager layoutManager;
    protected LedgerManagerFactory lmFactory;

    public String getScheme() {
        return SCHEME;
    }

    /**
     * Initialize metadata driver with provided configuration and <tt>statsLogger</tt>.
     *
     * @param conf configuration to initialize metadata driver
     * @param statsLogger stats logger
     * @throws MetadataException
     */
    protected void initialize(AbstractConfiguration<?> conf, StatsLogger statsLogger)
        throws MetadataException {
        this.conf = conf;
        this.statsLogger = statsLogger;

        final String metadataServiceUriStr;
        try {
            metadataServiceUriStr = conf.getMetadataServiceUri();
        } catch (ConfigurationException ce) {
            log.error("Failed to retrieve metadata service uri from configuration", ce);
            throw new MetadataException(Code.INVALID_METADATA_SERVICE_URI, ce);
        }
        ServiceURI serviceURI = ServiceURI.create(metadataServiceUriStr);
        this.keyPrefix = serviceURI.getServicePath();

        List<String> etcdEndpoints = Lists.newArrayList(serviceURI.getServiceHosts())
            .stream()
            .map(host -> String.format("http://%s", host))
            .collect(Collectors.toList());

        log.info("Initializing etcd metadata driver : etcd endpoints = {}, key scope = {}",
            etcdEndpoints, keyPrefix);

        synchronized (this) {
            this.client = Client.builder()
                .endpoints(etcdEndpoints.toArray(new String[etcdEndpoints.size()]))
                .build();
        }

        this.layoutManager = new EtcdLayoutManager(
            client,
            keyPrefix
        );
    }

    public LayoutManager getLayoutManager() {
        return layoutManager;
    }

    public synchronized LedgerManagerFactory getLedgerManagerFactory()
            throws MetadataException {
        if (null == lmFactory) {
            try {
                lmFactory = new EtcdLedgerManagerFactory();
                lmFactory.initialize(conf, layoutManager, EtcdLedgerManagerFactory.VERSION);
            } catch (IOException ioe) {
                throw new MetadataException(
                    Code.METADATA_SERVICE_ERROR, "Failed to initialize ledger manager factory", ioe);
            }
        }
        return lmFactory;
    }

    @Override
    public synchronized void close() {
        if (null != lmFactory) {
            try {
                lmFactory.close();
            } catch (IOException e) {
                log.error("Failed to close ledger manager factory", e);
            }
            lmFactory = null;
        }
        if (null != client) {
            client.close();
            client = null;
        }
    }
}
