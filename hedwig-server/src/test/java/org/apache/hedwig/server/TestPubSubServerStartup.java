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
package org.apache.hedwig.server;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;

import junit.framework.Assert;

import org.apache.bookkeeper.test.PortManager;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.netty.PubSubServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Test;

public class TestPubSubServerStartup {

    private static Logger logger = LoggerFactory.getLogger(TestPubSubServerStartup.class);

    /**
     * Start-up zookeeper + pubsubserver reading from a config URL. Then stop
     * and cleanup.
     *
     * Loop over that.
     *
     * If the pubsub server does not wait for its zookeeper client to be
     * connected, the pubsub server will fail at startup.
     *
     */
    @Test
    public void testPubSubServerInstantiationWithConfig() throws Exception {
        for (int i = 0; i < 10; i++) {
            logger.info("iteration " + i);
            instantiateAndDestroyPubSubServer();
        }
    }

    private void instantiateAndDestroyPubSubServer() throws IOException, InterruptedException, ConfigurationException,
        MalformedURLException, Exception {
        int zkPort = PortManager.nextFreePort();
        int hwPort = PortManager.nextFreePort();
        int hwSSLPort = PortManager.nextFreePort();
        String hedwigParams = "default_server_host=localhost:" + hwPort + "\n"
            + "zk_host=localhost:" + zkPort + "\n"
            + "server_port=" + hwPort + "\n"
            + "ssl_server_port=" + hwSSLPort + "\n"
            + "zk_timeout=2000\n";

        File hedwigConfigFile = new File(System.getProperty("java.io.tmpdir") + "/hedwig.cfg");
        writeStringToFile(hedwigParams, hedwigConfigFile);

        ClientBase.setupTestEnv();
        File zkTmpDir = File.createTempFile("zookeeper", "test");
        zkTmpDir.delete();
        zkTmpDir.mkdir();

        ZooKeeperServer zks = new ZooKeeperServer(zkTmpDir, zkTmpDir, zkPort);

        NIOServerCnxnFactory serverFactory = new NIOServerCnxnFactory();
        serverFactory.configure(new InetSocketAddress(zkPort), 100);
        serverFactory.startup(zks);

        boolean b = ClientBase.waitForServerUp("127.0.0.1:" + zkPort, 5000);
        ServerConfiguration serverConf = new ServerConfiguration();
        serverConf.loadConf(hedwigConfigFile.toURI().toURL());

        logger.info("Zookeeper server up and running!");

        ZooKeeper zkc = new ZooKeeper("127.0.0.1:" + zkPort, 5000, null);

        // initialize the zk client with (fake) values
        zkc.create("/ledgers", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zkc.create("/ledgers/available", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        zkc.close();
        PubSubServer hedwigServer = null;
        try {
            logger.info("starting hedwig broker!");
            hedwigServer = new PubSubServer(serverConf);
            hedwigServer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Assert.assertNotNull("failed to instantiate hedwig pub sub server", hedwigServer);

        hedwigServer.shutdown();
        serverFactory.shutdown();

        zks.shutdown();

        zkTmpDir.delete();

        ClientBase.waitForServerDown("localhost:" + zkPort, 10000);

    }

    public static void writeStringToFile(String string, File f) throws IOException {
        if (f.exists()) {
            if (!f.delete()) {
                throw new RuntimeException("cannot create file " + f.getAbsolutePath());
            }
        }
        if (!f.createNewFile()) {
            throw new RuntimeException("cannot create new file " + f.getAbsolutePath());
        }

        FileWriter fw = new FileWriter(f);
        fw.write(string);
        fw.close();
    }
}
