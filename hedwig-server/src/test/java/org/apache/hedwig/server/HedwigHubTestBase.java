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

import java.util.LinkedList;
import java.util.List;

import junit.framework.TestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Before;

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.netty.PubSubServer;
import org.apache.hedwig.server.persistence.BookKeeperTestBase;
import org.apache.hedwig.util.HedwigSocketAddress;

import org.apache.bookkeeper.test.PortManager;

/**
 * This is a base class for any tests that need a Hedwig Hub(s) setup with an
 * associated BookKeeper and ZooKeeper instance.
 *
 */
public abstract class HedwigHubTestBase extends TestCase {

    protected static Logger logger = LoggerFactory.getLogger(HedwigHubTestBase.class);

    // BookKeeper variables
    // Default number of bookie servers to setup. Extending classes can
    // override this.
    protected int numBookies = 3;
    protected long readDelay = 0L;
    protected BookKeeperTestBase bktb;

    // PubSubServer variables
    // Default number of PubSubServer hubs to setup. Extending classes can
    // override this.
    protected final int numServers;
    protected List<PubSubServer> serversList;
    protected List<HedwigSocketAddress> serverAddresses;

    public HedwigHubTestBase() {
        this(1);
    }

    protected HedwigHubTestBase(int numServers) {
        this.numServers = numServers;

        serverAddresses = new LinkedList<HedwigSocketAddress>();
        for (int i = 0; i < numServers; i++) {
            serverAddresses.add(new HedwigSocketAddress("localhost",
                                        PortManager.nextFreePort(), PortManager.nextFreePort()));
        }
    }

    // Default child class of the ServerConfiguration to be used here.
    // Extending classes can define their own (possibly extending from this) and
    // override the getServerConfiguration method below to return their own
    // configuration.
    protected class HubServerConfiguration extends ServerConfiguration {
        private final int serverPort, sslServerPort;

        public HubServerConfiguration(int serverPort, int sslServerPort) {
            this.serverPort = serverPort;
            this.sslServerPort = sslServerPort;
        }

        @Override
        public int getServerPort() {
            return serverPort;
        }

        @Override
        public int getSSLServerPort() {
            return sslServerPort;
        }

        @Override
        public String getZkHost() {
            return bktb.getZkHostPort();
        }

        @Override
        public boolean isSSLEnabled() {
            return true;
        }

        @Override
        public String getCertName() {
            return "/server.p12";
        }

        @Override
        public String getPassword() {
            return "eUySvp2phM2Wk";
        }
    }

    public class HubClientConfiguration extends ClientConfiguration {
        @Override
        public HedwigSocketAddress getDefaultServerHedwigSocketAddress() {
            return serverAddresses.get(0);
        }
    }

    // Method to get a ServerConfiguration for the PubSubServers created using
    // the specified ports. Extending child classes can override this. This
    // default implementation will return the HubServerConfiguration object
    // defined above.
    protected ServerConfiguration getServerConfiguration(int serverPort, int sslServerPort) {
        return new HubServerConfiguration(serverPort, sslServerPort);
    }

    protected void startHubServers() throws Exception {
        // Now create the PubSubServer Hubs
        serversList = new LinkedList<PubSubServer>();

        for (int i = 0; i < numServers; i++) {
            ServerConfiguration conf = getServerConfiguration(serverAddresses.get(i).getPort(),
                                                              serverAddresses.get(i).getSSLPort());
            PubSubServer s = new PubSubServer(conf, new ClientConfiguration(), new LoggingExceptionHandler());
            serversList.add(s);
            s.start();
        }
    }
    protected void stopHubServers() throws Exception {
        // Shutdown all of the PubSubServers
        for (PubSubServer server : serversList) {
            server.shutdown();
        }
        serversList.clear();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        logger.info("STARTING " + getName());
        bktb = new BookKeeperTestBase(numBookies, readDelay);
        bktb.setUp();
        startHubServers();
        logger.info("HedwigHub test setup finished");
    }

    @Override
    @After
    public void tearDown() throws Exception {
        logger.info("tearDown starting");
        stopHubServers();
        bktb.tearDown();
        logger.info("FINISHED " + getName());
    }

}
