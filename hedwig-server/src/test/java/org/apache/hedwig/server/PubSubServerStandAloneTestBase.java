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

import junit.framework.TestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Before;

import org.apache.bookkeeper.test.PortManager;

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.server.LoggingExceptionHandler;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.netty.PubSubServer;
import org.apache.hedwig.util.HedwigSocketAddress;

/**
 * This is a base class for any tests that need a StandAlone PubSubServer setup.
 */
public abstract class PubSubServerStandAloneTestBase extends TestCase {

    protected static Logger logger = LoggerFactory.getLogger(PubSubServerStandAloneTestBase.class);

    protected class StandAloneServerConfiguration extends ServerConfiguration {
        final int port = PortManager.nextFreePort();
        final int sslPort = PortManager.nextFreePort();

        @Override
        public boolean isStandalone() {
            return true;
        }

        @Override
        public int getServerPort() {
            return port;
        }

        @Override
        public int getSSLServerPort() {
            return sslPort;
        }
    }

    public ServerConfiguration getStandAloneServerConfiguration() {
        return new StandAloneServerConfiguration();
    }

    protected PubSubServer server;
    protected ServerConfiguration conf;
    protected HedwigSocketAddress defaultAddress;

    @Override
    @Before
    public void setUp() throws Exception {
        logger.info("STARTING " + getName());
        conf = getStandAloneServerConfiguration();
        startHubServer(conf);
        logger.info("Standalone PubSubServer test setup finished");
    }


    @Override
    @After
    public void tearDown() throws Exception {
        logger.info("tearDown starting");
        tearDownHubServer();
        logger.info("FINISHED " + getName());
    }

    protected HedwigSocketAddress getDefaultHedwigAddress() {
        return defaultAddress;
    }

    protected void startHubServer(ServerConfiguration conf) throws Exception {
        defaultAddress = new HedwigSocketAddress("localhost", conf.getServerPort(),
                                                 conf.getSSLServerPort());
        server = new PubSubServer(conf, new ClientConfiguration(), new LoggingExceptionHandler());
        server.start();
    }

    protected void tearDownHubServer() throws Exception {
        server.shutdown();
    }

}
