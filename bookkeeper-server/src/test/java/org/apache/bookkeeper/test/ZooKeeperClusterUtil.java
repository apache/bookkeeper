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
package org.apache.bookkeeper.test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.QuorumUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides multi node zookeeper cluster.
 */
@Slf4j
public class ZooKeeperClusterUtil implements ZooKeeperCluster {

    static {
        enableZookeeperTestEnvVariables();
    }

    static final Logger LOG = LoggerFactory.getLogger(ZooKeeperClusterUtil.class);
    private final int numOfZKNodes;
    public QuorumUtil quorumUtil;
    String connectString;
    protected ZooKeeper zkc; // zookeeper client

    public static void enableZookeeperTestEnvVariables() {
        /*
         * org.apache.zookeeper.test.ClientBase uses FourLetterWordMain, from
         * 3.5.3 four letter words are disabled by default due to security
         * reasons
         */
        System.setProperty("zookeeper.4lw.commands.whitelist", "*");
        System.setProperty("zookeeper.admin.enableServer", "false");
        try {
            System.setProperty("build.test.dir", Files.createTempDirectory("zktests").toFile().getCanonicalPath());
        } catch (IOException e) {
            log.error("Failed to create temp dir, so setting build.test.dir system property to /tmp");
            System.setProperty("build.test.dir", "/tmp");
        }
    }

    public ZooKeeperClusterUtil(int numOfZKNodes) throws IOException, KeeperException, InterruptedException {
        if ((numOfZKNodes < 3) || (numOfZKNodes % 2 == 0)) {
            throw new IllegalArgumentException("numOfZKNodes should be atleast 3 and it should not be even number");
        }
        this.numOfZKNodes = numOfZKNodes;
    }

    @Override
    public String getZooKeeperConnectString() {
        return connectString;
    }

    @Override
    public String getMetadataServiceUri() {
        return getMetadataServiceUri("/ledgers");
    }

    @Override
    public String getMetadataServiceUri(String zkLedgersRootPath) {
        return getMetadataServiceUri(zkLedgersRootPath, LongHierarchicalLedgerManagerFactory.NAME);
    }

    @Override
    public String getMetadataServiceUri(String zkLedgersRootPath, String type) {
        /*
         * URI doesn't accept ',', for more info. check
         * AbstractConfiguration.getMetadataServiceUri()
         */
        return "zk+" + type + "://" + connectString.replace(",", ";") + zkLedgersRootPath;
    }

    @Override
    public ZooKeeper getZooKeeperClient() {
        return zkc;
    }

    @Override
    public void startCluster() throws Exception {
        // QuorumUtil will start 2*n+1 nodes.
        quorumUtil = new QuorumUtil(numOfZKNodes / 2);
        quorumUtil.startAll();
        connectString = quorumUtil.getConnString();
        // create a zookeeper client
        if (LOG.isDebugEnabled()) {
            LOG.debug("Instantiate ZK Client");
        }
        zkc = ZooKeeperClient.newBuilder().connectString(getZooKeeperConnectString()).sessionTimeoutMs(10000).build();

        // create default bk ensemble
        createBKEnsemble("/ledgers");
    }

    @Override
    public void stopCluster() throws Exception {
        if (zkc != null) {
            zkc.close();
        }
        quorumUtil.shutdownAll();
    }

    @Override
    public void restartCluster() throws Exception {
        quorumUtil.startAll();
    }

    @Override
    public void killCluster() throws Exception {
        quorumUtil.tearDown();
    }

    @Override
    public void sleepCluster(int time, TimeUnit timeUnit, CountDownLatch l) throws InterruptedException, IOException {
        throw new UnsupportedOperationException("sleepServer operation is not supported for ZooKeeperClusterUtil");
    }

    public void stopPeer(int id) throws Exception {
        quorumUtil.shutdown(id);
    }

    public void enableLocalSession(boolean localSessionEnabled) {
        quorumUtil.enableLocalSession(localSessionEnabled);
    }
}
