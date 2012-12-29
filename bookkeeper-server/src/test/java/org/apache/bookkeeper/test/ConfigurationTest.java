/**
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

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;

import junit.framework.TestCase;

import org.junit.Test;

public class ConfigurationTest extends TestCase {
    @Test(timeout=60000)
    public void testConfigurationOverwrite() {
        System.clearProperty("zkServers");

        ServerConfiguration conf = new ServerConfiguration();
        assertEquals(null, conf.getZkServers());

        // override setting from property
        System.setProperty("zkServers", "server1");
        // it affects previous created configurations, if the setting is not overwrite
        assertEquals("server1", conf.getZkServers());

        ServerConfiguration conf2 = new ServerConfiguration();
        assertEquals("server1", conf2.getZkServers());

        System.clearProperty("zkServers");

        // load other configuration
        ServerConfiguration newConf = new ServerConfiguration();
        assertEquals(null, newConf.getZkServers());
        newConf.setZkServers("newserver");
        assertEquals("newserver", newConf.getZkServers());
        conf2.loadConf(newConf);
        assertEquals("newserver", conf2.getZkServers());
    }

    @Test(timeout=60000)
    public void testGetZkServers() {
        System.setProperty("zkServers", "server1:port1,server2:port2");
        ServerConfiguration conf = new ServerConfiguration();
        ClientConfiguration clientConf = new ClientConfiguration();
        assertEquals("zookeeper connect string doesn't match in server configuration",
                     "server1:port1,server2:port2", conf.getZkServers());
        assertEquals("zookeeper connect string doesn't match in client configuration",
                     "server1:port1,server2:port2", clientConf.getZkServers());
    }
}
