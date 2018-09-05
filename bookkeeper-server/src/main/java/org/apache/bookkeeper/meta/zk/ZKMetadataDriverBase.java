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
package org.apache.bookkeeper.meta.zk;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.EMPTY_BYTE_ARRAY;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.AbstractZkLedgerManagerFactory;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.ZkLayoutManager;
import org.apache.bookkeeper.meta.exceptions.Code;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * This is a mixin class for supporting zookeeper based metadata driver.
 */
@Slf4j
public class ZKMetadataDriverBase implements AutoCloseable {

    protected static final String SCHEME = "zk";

    public static String getZKServersFromServiceUri(URI uri) {
        return uri.getAuthority().replace(";", ",");
    }

    @SuppressWarnings("deprecation")
    public static String resolveZkServers(AbstractConfiguration<?> conf) {
        String metadataServiceUriStr = conf.getMetadataServiceUriUnchecked();
        if (null == metadataServiceUriStr) {
            return conf.getZkServers();
        }
        URI metadataServiceUri = URI.create(metadataServiceUriStr);
        return getZKServersFromServiceUri(metadataServiceUri);
    }

    @SuppressWarnings("deprecation")
    public static String resolveZkLedgersRootPath(AbstractConfiguration<?> conf) {
        String metadataServiceUriStr = conf.getMetadataServiceUriUnchecked();
        if (null == metadataServiceUriStr) {
            return conf.getZkLedgersRootPath();
        }
        URI metadataServiceUri = URI.create(metadataServiceUriStr);
        return metadataServiceUri.getPath();
    }

    @SuppressWarnings("deprecation")
    public static Class<? extends LedgerManagerFactory> resolveLedgerManagerFactory(URI metadataServiceUri) {
        checkNotNull(metadataServiceUri, "Metadata service uri is null");
        String scheme = metadataServiceUri.getScheme();
        checkNotNull(scheme, "Invalid metadata service : " + metadataServiceUri);
        String[] schemeParts = StringUtils.split(scheme.toLowerCase(), '+');
        checkArgument(SCHEME.equals(schemeParts[0]), "Unknown metadata service scheme found : "
            + schemeParts[0]);
        Class<? extends LedgerManagerFactory> ledgerManagerFactoryClass;
        if (schemeParts.length > 1) {
            switch (schemeParts[1]) {
                case org.apache.bookkeeper.meta.FlatLedgerManagerFactory.NAME:
                    ledgerManagerFactoryClass = org.apache.bookkeeper.meta.FlatLedgerManagerFactory.class;
                    break;
                case HierarchicalLedgerManagerFactory.NAME:
                    ledgerManagerFactoryClass = HierarchicalLedgerManagerFactory.class;
                    break;
                case LongHierarchicalLedgerManagerFactory.NAME:
                    ledgerManagerFactoryClass = LongHierarchicalLedgerManagerFactory.class;
                    break;
                case org.apache.bookkeeper.meta.MSLedgerManagerFactory.NAME:
                    ledgerManagerFactoryClass = org.apache.bookkeeper.meta.MSLedgerManagerFactory.class;
                    break;
                case "null":
                    // the ledger manager factory class is not set, so the client will be using the class that is
                    // recorded in ledger manager layout.
                    ledgerManagerFactoryClass = null;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown ledger manager type found '"
                        + schemeParts[1] + "' at uri : " + metadataServiceUri);
            }
        } else {
            // behave as in the +null case, infer the layout from the store or fall back to the default
            ledgerManagerFactoryClass = null;
        }
        return ledgerManagerFactoryClass;
    }

    // URI
    protected AbstractConfiguration<?> conf;
    protected StatsLogger statsLogger;

    // zookeeper related variables
    protected List<ACL> acls;
    @Getter
    @Setter
    protected ZooKeeper zk = null;
    // whether the zk handle is one we created, or is owned by whoever
    // instantiated us
    protected boolean ownZKHandle = false;

    // ledgers root path
    protected String ledgersRootPath;

    // managers
    protected LayoutManager layoutManager;
    protected LedgerManagerFactory lmFactory;

    public String getScheme() {
        return SCHEME;
    }

    @SuppressWarnings("deprecation")
    @SneakyThrows(InterruptedException.class)
    protected void initialize(AbstractConfiguration<?> conf,
                              StatsLogger statsLogger,
                              RetryPolicy zkRetryPolicy,
                              Optional<Object> optionalCtx) throws MetadataException {
        this.conf = conf;
        this.acls = ZkUtils.getACLs(conf);

        if (optionalCtx.isPresent()
            && optionalCtx.get() instanceof ZooKeeper) {
            this.ledgersRootPath = conf.getZkLedgersRootPath();

            log.info("Initialize zookeeper metadata driver with external zookeeper client : ledgersRootPath = {}.",
                ledgersRootPath);

            // if an external zookeeper is added, use the zookeeper instance
            this.zk = (ZooKeeper) (optionalCtx.get());
            this.ownZKHandle = false;
        } else {
            final String metadataServiceUriStr;
            try {
                metadataServiceUriStr = conf.getMetadataServiceUri();
            } catch (ConfigurationException e) {
                log.error("Failed to retrieve metadata service uri from configuration", e);
                throw new MetadataException(
                    Code.INVALID_METADATA_SERVICE_URI, e);
            }

            URI metadataServiceUri = URI.create(metadataServiceUriStr);
            // get the initialize root path
            this.ledgersRootPath = metadataServiceUri.getPath();
            final String bookieRegistrationPath = ledgersRootPath + "/" + AVAILABLE_NODE;
            final String bookieReadonlyRegistrationPath = bookieRegistrationPath + "/" + READONLY;

            // construct the zookeeper
            final String zkServers = getZKServersFromServiceUri(metadataServiceUri);
            log.info("Initialize zookeeper metadata driver at metadata service uri {} :"
                + " zkServers = {}, ledgersRootPath = {}.", metadataServiceUriStr, zkServers, ledgersRootPath);

            try {
                this.zk = ZooKeeperClient.newBuilder()
                    .connectString(zkServers)
                    .sessionTimeoutMs(conf.getZkTimeout())
                    .operationRetryPolicy(zkRetryPolicy)
                    .requestRateLimit(conf.getZkRequestRateLimit())
                    .statsLogger(statsLogger)
                    .build();

                if (null == zk.exists(bookieReadonlyRegistrationPath, false)) {
                    try {
                        zk.create(bookieReadonlyRegistrationPath,
                            EMPTY_BYTE_ARRAY,
                            acls,
                            CreateMode.PERSISTENT);
                    } catch (KeeperException.NodeExistsException e) {
                        // this node is just now created by someone.
                    } catch (KeeperException.NoNodeException e) {
                        // the cluster hasn't been initialized
                    }
                }
            } catch (IOException | KeeperException e) {
                log.error("Failed to create zookeeper client to {}", zkServers, e);
                MetadataException me = new MetadataException(
                    Code.METADATA_SERVICE_ERROR,
                    "Failed to create zookeeper client to " + zkServers,
                    e);
                me.fillInStackTrace();
                throw me;
            }
            this.ownZKHandle = true;
        }

        // once created the zookeeper client, create the layout manager and registration client
        this.layoutManager = new ZkLayoutManager(
            zk,
            ledgersRootPath,
            acls);
    }

    public LayoutManager getLayoutManager() {
        return layoutManager;
    }

    @SneakyThrows
    public synchronized LedgerManagerFactory getLedgerManagerFactory()
            throws MetadataException {
        if (null == lmFactory) {
            try {
                lmFactory = AbstractZkLedgerManagerFactory.newLedgerManagerFactory(
                    conf,
                    layoutManager);
            } catch (IOException e) {
                throw new MetadataException(
                    Code.METADATA_SERVICE_ERROR, "Failed to initialized ledger manager factory", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
        }
        return lmFactory;
    }

    @Override
    public void close() {
        if (null != lmFactory) {
            try {
                lmFactory.close();
            } catch (IOException e) {
                log.warn("Failed to close zookeeper based ledger manager", e);
            }
            lmFactory = null;
        }
        if (ownZKHandle && null != zk) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted on closing zookeeper client", e);
            }
            zk = null;
        }
    }
}
