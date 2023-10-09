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

import static org.junit.Assert.assertEquals;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.Test;

/**
 * Test the configuration class.
 */
public class ConfigurationTest {

    static {
        // this property is read when AbstractConfiguration class is loaded.
        // this test will work as expected only using a new JVM (or classloader) for the test
        System.setProperty(AbstractConfiguration.READ_SYSTEM_PROPERTIES_PROPERTY, "true");
    }

    @Test
    public void testConfigurationOverwrite() {
        System.clearProperty("metadataServiceUri");

        ServerConfiguration conf = new ServerConfiguration();
        assertEquals(null, conf.getMetadataServiceUriUnchecked());

        // override setting from property
        System.setProperty("metadataServiceUri", "zk://server:2181/ledgers");
        // it affects previous created configurations, if the setting is not overwrite
        assertEquals("zk://server:2181/ledgers", conf.getMetadataServiceUriUnchecked());

        ServerConfiguration conf2 = new ServerConfiguration();
        assertEquals("zk://server:2181/ledgers", conf2.getMetadataServiceUriUnchecked());

        System.clearProperty("metadataServiceUri");

        // load other configuration
        ServerConfiguration newConf = new ServerConfiguration();
        assertEquals(null, newConf.getMetadataServiceUriUnchecked());
        newConf.setMetadataServiceUri("zk://newserver:2181/ledgers");
        assertEquals("zk://newserver:2181/ledgers", newConf.getMetadataServiceUriUnchecked());
        conf2.loadConf(newConf);
        assertEquals("zk://newserver:2181/ledgers", conf2.getMetadataServiceUriUnchecked());
    }

    @Test
    public void testGetZkServers() {
        System.setProperty("metadataServiceUri", "zk://server1:port1;server2:port2/ledgers");
        ServerConfiguration conf = new ServerConfiguration();
        ClientConfiguration clientConf = new ClientConfiguration();
        assertEquals("zookeeper connect string doesn't match in server configuration",
                     "zk://server1:port1;server2:port2/ledgers", conf.getMetadataServiceUriUnchecked());
        assertEquals("zookeeper connect string doesn't match in client configuration",
                     "zk://server1:port1;server2:port2/ledgers", clientConf.getMetadataServiceUriUnchecked());
    }
}
