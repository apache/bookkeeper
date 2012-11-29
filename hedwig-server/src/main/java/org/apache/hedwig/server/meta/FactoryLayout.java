package org.apache.hedwig.server.meta;

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.TextFormat;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hedwig.protocol.PubSubProtocol.ManagerMeta;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.zookeeper.ZkUtils;

/**
 * This class encapsulates metadata manager layout information
 * that is persistently stored in zookeeper.
 * It provides parsing and serialization methods of such information.
 *
 */
public class FactoryLayout {
    static final Logger logger = LoggerFactory.getLogger(FactoryLayout.class);

    // metadata manager name
    public static final String NAME = "METADATA";
    // Znode name to store layout information
    public static final String LAYOUT_ZNODE = "LAYOUT";
    public static final String LSEP = "\n";

    private ManagerMeta managerMeta;

    /**
     * Construct metadata manager factory layout.
     *
     * @param meta
     *          Meta describes what kind of factory used.
     */
    public FactoryLayout(ManagerMeta meta) {
        this.managerMeta = meta;
    }

    public static String getFactoryLayoutPath(StringBuilder sb, ServerConfiguration cfg) {
        return cfg.getZkManagersPrefix(sb).append("/").append(NAME)
               .append("/").append(LAYOUT_ZNODE).toString();
    }

    public ManagerMeta getManagerMeta() {
        return managerMeta;
    }

    /**
     * Store the factory layout into zookeeper
     *
     * @param zk
     *          ZooKeeper Handle
     * @param cfg
     *          Server Configuration Object
     * @throws KeeperException
     * @throws IOException
     * @throws InterruptedException
     */
    public void store(ZooKeeper zk, ServerConfiguration cfg)
    throws KeeperException, IOException, InterruptedException {
        String factoryLayoutPath = getFactoryLayoutPath(new StringBuilder(), cfg);

        byte[] layoutData = TextFormat.printToString(managerMeta).getBytes();
        ZkUtils.createFullPathOptimistic(zk, factoryLayoutPath, layoutData,
                                         Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Override
    public int hashCode() {
        return managerMeta.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (null == o ||
            !(o instanceof FactoryLayout)) {
            return false;
        }
        FactoryLayout other = (FactoryLayout)o;
        return managerMeta.equals(other.managerMeta);
    }

    @Override
    public String toString() {
        return TextFormat.printToString(managerMeta);
    }

    /**
     * Read factory layout from zookeeper
     *
     * @param zk
     *          ZooKeeper Client
     * @param cfg
     *          Server configuration object
     * @return Factory layout, or null if none set in zookeeper
     */
    public static FactoryLayout readLayout(final ZooKeeper zk,
                                           final ServerConfiguration cfg)
    throws IOException, KeeperException {
        String factoryLayoutPath = getFactoryLayoutPath(new StringBuilder(), cfg);
        byte[] layoutData;
        try {
            layoutData = zk.getData(factoryLayoutPath, false, null);
        } catch (KeeperException.NoNodeException nne) {
            return null;
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        }
        ManagerMeta meta;
        try {
            BufferedReader reader = new BufferedReader(
                new StringReader(new String(layoutData)));
            ManagerMeta.Builder metaBuilder = ManagerMeta.newBuilder();
            TextFormat.merge(reader, metaBuilder);
            meta = metaBuilder.build();
        } catch (InvalidProtocolBufferException ipbe) {
            throw new IOException("Corrupted factory layout : ", ipbe);
        }

        return new FactoryLayout(meta);
    }

    /**
     * Remove the factory layout from ZooKeeper.
     *
     * @param zk
     *          ZooKeeper instance
     * @param cfg
     *          Server configuration object
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void deleteLayout(ZooKeeper zk, ServerConfiguration cfg)
            throws KeeperException, InterruptedException {
        String factoryLayoutPath = getFactoryLayoutPath(new StringBuilder(), cfg);
        zk.delete(factoryLayoutPath, -1);
    }

}
