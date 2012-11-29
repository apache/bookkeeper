/**
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
package org.apache.hedwig.server.meta;

import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.hedwig.protocol.PubSubProtocol.ManagerMeta;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.google.protobuf.ByteString;

/**
 * Metadata Manager used to manage metadata used by hedwig.
 */
public abstract class MetadataManagerFactory {

    static final Logger LOG = LoggerFactory.getLogger(MetadataManagerFactory.class);

    /**
     * Return current factory version.
     *
     * @return current version used by factory.
     */
    public abstract int getCurrentVersion();

    /**
     * Initialize the metadata manager factory with given
     * configuration and version.
     *
     * @param cfg
     *          Server configuration object
     * @param zk
     *          ZooKeeper handler
     * @param version
     *          Manager version
     * @return metadata manager factory
     * @throws IOException when fail to initialize the manager.
     */
    protected abstract MetadataManagerFactory initialize(
        ServerConfiguration cfg, ZooKeeper zk, int version)
    throws IOException;

    /**
     * Uninitialize the factory.
     *
     * @throws IOException when fail to shutdown the factory.
     */
    public abstract void shutdown() throws IOException;

    /**
     * Iterate over the topics list.
     * Used by HedwigConsole to list available topics.
     *
     * @return iterator of the topics list.
     * @throws IOException
     */
    public abstract Iterator<ByteString> getTopics() throws IOException;

    /**
     * Create topic persistence manager.
     *
     * @return topic persistence manager
     */
    public abstract TopicPersistenceManager newTopicPersistenceManager();

    /**
     * Create subscription data manager.
     *
     * @return subscription data manager.
     */
    public abstract SubscriptionDataManager newSubscriptionDataManager();

    /**
     * Create topic ownership manager.
     *
     * @return topic ownership manager.
     */
    public abstract TopicOwnershipManager newTopicOwnershipManager();

    /**
     * Format the metadata for Hedwig.
     *
     * @param cfg
     *          Configuration instance
     * @param zk
     *          ZooKeeper instance
     */
    public abstract void format(ServerConfiguration cfg, ZooKeeper zk) throws IOException;

    /**
     * Create new Metadata Manager Factory.
     *
     * @param conf
     *          Configuration Object.
     * @param zk
     *          ZooKeeper Client Handle, talk to zk to know which manager factory is used.
     * @return new manager factory.
     * @throws IOException
     */
    public static MetadataManagerFactory newMetadataManagerFactory(
        final ServerConfiguration conf, final ZooKeeper zk)
    throws IOException, KeeperException, InterruptedException {
        Class<? extends MetadataManagerFactory> factoryClass;
        try {
            factoryClass = conf.getMetadataManagerFactoryClass();
        } catch (Exception e) {
            throw new IOException("Failed to get metadata manager factory class from configuration : ", e);
        }
        // check that the configured manager is
        // compatible with the existing layout
        FactoryLayout layout = FactoryLayout.readLayout(zk, conf);
        if (layout == null) { // no existing layout
            return createMetadataManagerFactory(conf, zk, factoryClass);
        }
        LOG.debug("read meta layout {}", layout);

        if (factoryClass != null &&
            !layout.getManagerMeta().getManagerImpl().equals(factoryClass.getName())) {
            throw new IOException("Configured metadata manager factory " + factoryClass.getName()
                                + " does not match existing factory "  + layout.getManagerMeta().getManagerImpl());
        }
        if (factoryClass == null) {
            // no factory specified in configuration
            String factoryClsName = layout.getManagerMeta().getManagerImpl();
            try {
                Class<?> theCls = Class.forName(factoryClsName);
                if (!MetadataManagerFactory.class.isAssignableFrom(theCls)) {
                    throw new IOException("Wrong metadata manager factory " + factoryClsName);
                }
                factoryClass = theCls.asSubclass(MetadataManagerFactory.class);
            } catch (ClassNotFoundException cnfe) {
                throw new IOException("No class found to instantiate metadata manager factory " + factoryClsName);
            }
        }
        // instantiate the metadata manager factory
        MetadataManagerFactory managerFactory;
        try {
            managerFactory = ReflectionUtils.newInstance(factoryClass);
        } catch (Throwable t) {
            throw new IOException("Failed to instantiate metadata manager factory : " + factoryClass, t);
        }
        return managerFactory.initialize(conf, zk, layout.getManagerMeta().getManagerVersion());
    }

    /**
     * Create metadata manager factory and write factory layout to ZooKeeper.
     *
     * @param cfg
     *          Server Configuration object.
     * @param zk
     *          ZooKeeper instance.
     * @param factoryClass
     *          Metadata Manager Factory Class.
     * @return metadata manager factory instance.
     * @throws IOException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static MetadataManagerFactory createMetadataManagerFactory(
            ServerConfiguration cfg, ZooKeeper zk,
            Class<? extends MetadataManagerFactory> factoryClass)
            throws IOException, KeeperException, InterruptedException {
        // use default manager if no one provided
        if (factoryClass == null) {
            factoryClass = ZkMetadataManagerFactory.class;
        }

        MetadataManagerFactory managerFactory;
        try {
            managerFactory = ReflectionUtils.newInstance(factoryClass);
        } catch (Throwable t) {
            throw new IOException("Fail to instantiate metadata manager factory : " + factoryClass, t);
        }
        ManagerMeta managerMeta = ManagerMeta.newBuilder()
                                  .setManagerImpl(factoryClass.getName())
                                  .setManagerVersion(managerFactory.getCurrentVersion())
                                  .build();
        FactoryLayout layout = new FactoryLayout(managerMeta);
        try {
            layout.store(zk, cfg);
        } catch (KeeperException.NodeExistsException nee) {
            FactoryLayout layout2 = FactoryLayout.readLayout(zk, cfg);
            if (!layout2.equals(layout)) {
                throw new IOException("Contention writing to layout to zookeeper, "
                        + " other layout " + layout2 + " is incompatible with our "
                        + "layout " + layout);
            }
        }
        return managerFactory.initialize(cfg, zk, layout.getManagerMeta().getManagerVersion());
    }
}
