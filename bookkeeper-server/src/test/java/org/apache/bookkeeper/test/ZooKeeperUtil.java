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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.test.ClientBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the zookeeper utilities.
 */
public class ZooKeeperUtil implements ZooKeeperCluster {

    static {
        // org.apache.zookeeper.test.ClientBase uses FourLetterWordMain, from 3.5.3 four letter words
        // are disabled by default due to security reasons
        System.setProperty("zookeeper.4lw.commands.whitelist", "*");
    }
    static final Logger LOG = LoggerFactory.getLogger(ZooKeeperUtil.class);

    // ZooKeeper related variables
    protected Integer zooKeeperPort = 0;
    private InetSocketAddress zkaddr;

    protected ZooKeeperServer zks;
    protected ZooKeeper zkc; // zookeeper client
    protected NIOServerCnxnFactory serverFactory;
    protected File zkTmpDir;
    private String connectString;

    public ZooKeeperUtil() {
        String loopbackIPAddr = InetAddress.getLoopbackAddress().getHostAddress();
        zkaddr = new InetSocketAddress(loopbackIPAddr, 0);
        connectString = loopbackIPAddr + ":" + zooKeeperPort;
    }

    @Override
    public ZooKeeper getZooKeeperClient() {
        return zkc;
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
        return "zk://" + connectString + zkLedgersRootPath;
    }

    @Override
    public String getMetadataServiceUri(String zkLedgersRootPath, String type) {
        return "zk+" + type + "://" + connectString + zkLedgersRootPath;
    }

    @Override
    public void startCluster() throws Exception {
        // create a ZooKeeper server(dataDir, dataLogDir, port)
        if (LOG.isDebugEnabled()) {
            LOG.debug("Running ZK server");
        }
        ClientBase.setupTestEnv();
        zkTmpDir = IOUtils.createTempDir("zookeeper", "test");

        // start the server and client.
        restartCluster();

        // create default bk ensemble
        createBKEnsemble("/ledgers");
    }

    @Override
    public void restartCluster() throws Exception {
        zks = new ZooKeeperServer(zkTmpDir, zkTmpDir,
                ZooKeeperServer.DEFAULT_TICK_TIME);
        serverFactory = new NIOServerCnxnFactory();
        serverFactory.configure(zkaddr, 100);
        serverFactory.startup(zks);

        if (0 == zooKeeperPort) {
            zooKeeperPort = serverFactory.getLocalPort();
            zkaddr = new InetSocketAddress(zkaddr.getHostName(), zooKeeperPort);
            connectString = zkaddr.getHostName() + ":" + zooKeeperPort;
        }

        boolean b = ClientBase.waitForServerUp(getZooKeeperConnectString(),
                ClientBase.CONNECTION_TIMEOUT);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Server up: " + b);
        }

        // create a zookeeper client
        if (LOG.isDebugEnabled()) {
            LOG.debug("Instantiate ZK Client");
        }
        zkc = ZooKeeperClient.newBuilder()
                .connectString(getZooKeeperConnectString())
                .sessionTimeoutMs(10000)
                .build();
    }

    @Override
    public void sleepCluster(final int time,
                            final TimeUnit timeUnit,
                            final CountDownLatch l)
            throws InterruptedException, IOException {
        Thread[] allthreads = new Thread[Thread.activeCount()];
        Thread.enumerate(allthreads);
        for (final Thread t : allthreads) {
            if (t.getName().contains("SyncThread:0")) {
                Thread sleeper = new Thread() {
                    @SuppressWarnings("deprecation")
                    public void run() {
                        try {
                            t.suspend();
                            l.countDown();
                            timeUnit.sleep(time);
                            t.resume();
                        } catch (Exception e) {
                            LOG.error("Error suspending thread", e);
                        }
                    }
                };
                sleeper.start();
                return;
            }
        }
        throw new IOException("ZooKeeper thread not found");
    }

    @Override
    public void stopCluster() throws Exception {
        if (zkc != null) {
            zkc.close();
        }

        // shutdown ZK server
        if (serverFactory != null) {
            serverFactory.shutdown();
            assertTrue("waiting for server down",
                    ClientBase.waitForServerDown(getZooKeeperConnectString(),
                            ClientBase.CONNECTION_TIMEOUT));
        }
        if (zks != null) {
            zks.getTxnLogFactory().close();
        }
    }

    @Override
    public void killCluster() throws Exception {
        stopCluster();
        FileUtils.deleteDirectory(zkTmpDir);
    }
}
