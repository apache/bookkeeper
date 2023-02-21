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
package org.apache.bookkeeper.conf;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import org.apache.bookkeeper.common.allocator.LeakDetectionPolicy;
import org.apache.bookkeeper.meta.AbstractZkLedgerManagerFactory;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link AbstractConfiguration}.
 */
public class AbstractConfigurationTest {

    private static final String DEFAULT_METADATA_SERVICE_URI =
        "zk+null://127.0.0.1/path/to/ledgers";
    private static final String HIERARCHICAL_METADATA_SERVICE_URI =
        "zk+hierarchical://127.0.0.1/path/to/ledgers";
    private static final String FLAT_METADATA_SERVICE_URI =
        "zk+flat://127.0.0.1/path/to/ledgers";
    private static final String LONGHIERARCHICAL_METADATA_SERVICE_URI =
        "zk+longhierarchical://127.0.0.1/path/to/ledgers";
    private static final String MS_METADATA_SERVICE_URI =
        "zk+ms://127.0.0.1/path/to/ledgers";

    private AbstractConfiguration conf;

    @Before
    @SuppressWarnings("deprecation")
    public void setup() {
        this.conf = new ClientConfiguration();
        this.conf.setZkServers("127.0.0.1");
        this.conf.setZkLedgersRootPath("/path/to/ledgers");
    }

    @Test
    public void testDefaultServiceUri() throws Exception {
        assertEquals(
            DEFAULT_METADATA_SERVICE_URI,
            conf.getMetadataServiceUri());
    }

    @Test
    public void testSetMetadataServiceUri() throws Exception {
        assertEquals(
            DEFAULT_METADATA_SERVICE_URI,
            conf.getMetadataServiceUri());
        String serviceUri = "etcd://128.0.0.1/key/prefix";
        conf.setMetadataServiceUri(serviceUri);
        assertEquals(
            "Service URI should be changed to " + serviceUri,
            serviceUri,
            conf.getMetadataServiceUri());
    }

    @SuppressWarnings({ "unchecked" })
    @Test(expected = ConfigurationException.class)
    public void testUnsupportedLedgerManagerFactory() throws Exception {
        LedgerManagerFactory mockFactory = mock(LedgerManagerFactory.class, CALLS_REAL_METHODS);
        conf.setLedgerManagerFactoryClass(mockFactory.getClass());
        conf.getMetadataServiceUri();
    }

    @SuppressWarnings({ "deprecation", "unchecked" })
    @Test
    public void testFlatLedgerManagerUri() throws Exception {
        conf.setLedgerManagerFactoryClass(org.apache.bookkeeper.meta.FlatLedgerManagerFactory.class);
        assertEquals(
            FLAT_METADATA_SERVICE_URI,
            conf.getMetadataServiceUri());
    }

    @SuppressWarnings({ "unchecked" })
    @Test
    public void testHierarchicalLedgerManagerUri() throws Exception {
        conf.setLedgerManagerFactoryClass(HierarchicalLedgerManagerFactory.class);
        assertEquals(
            HIERARCHICAL_METADATA_SERVICE_URI,
            conf.getMetadataServiceUri());
    }

    @SuppressWarnings({ "unchecked" })
    @Test
    public void testLongHierarchicalLedgerManagerUri() throws Exception {
        conf.setLedgerManagerFactoryClass(LongHierarchicalLedgerManagerFactory.class);
        assertEquals(
            LONGHIERARCHICAL_METADATA_SERVICE_URI,
            conf.getMetadataServiceUri());
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
    @Test
    public void testMsLedgerManagerUri() throws Exception {
        conf.setLedgerManagerFactoryClass(
            org.apache.bookkeeper.meta.MSLedgerManagerFactory.class);
        assertEquals(
            MS_METADATA_SERVICE_URI,
            conf.getMetadataServiceUri());
    }

    @SuppressWarnings({ "unchecked" })
    @Test(expected = IllegalArgumentException.class)
    public void testUnknownZkLedgerManagerFactory() throws Exception {
        AbstractZkLedgerManagerFactory mockZkFactory =
            mock(AbstractZkLedgerManagerFactory.class, CALLS_REAL_METHODS);
        conf.setLedgerManagerFactoryClass(mockZkFactory.getClass());
        conf.getMetadataServiceUri();
    }

    @Test
    public void testAllocatorLeakDetectionPolicy() {
        String nettyOldLevelKey = "io.netty.leakDetectionLevel";
        String nettyLevelKey = "io.netty.leakDetection.level";

        String nettyOldLevelStr = System.getProperty(nettyOldLevelKey);
        String nettyLevelStr = System.getProperty(nettyLevelKey);

        //Remove netty property for test.
        System.getProperties().remove(nettyOldLevelKey);
        System.getProperties().remove(nettyLevelKey);

        assertEquals(LeakDetectionPolicy.Disabled, conf.getAllocatorLeakDetectionPolicy());

        System.getProperties().put(nettyOldLevelKey, "zazaza");
        assertEquals(LeakDetectionPolicy.Disabled, conf.getAllocatorLeakDetectionPolicy());

        conf.setProperty(AbstractConfiguration.ALLOCATOR_LEAK_DETECTION_POLICY, "zazaza");
        assertEquals(LeakDetectionPolicy.Disabled, conf.getAllocatorLeakDetectionPolicy());

        System.getProperties().put(nettyOldLevelKey, "simple");
        assertEquals(LeakDetectionPolicy.Simple, conf.getAllocatorLeakDetectionPolicy());

        System.getProperties().put(nettyLevelKey, "disabled");
        assertEquals(LeakDetectionPolicy.Disabled, conf.getAllocatorLeakDetectionPolicy());

        System.getProperties().put(nettyLevelKey, "advanCed");
        assertEquals(LeakDetectionPolicy.Advanced, conf.getAllocatorLeakDetectionPolicy());

        conf.setProperty(AbstractConfiguration.ALLOCATOR_LEAK_DETECTION_POLICY, "simPle");
        assertEquals(LeakDetectionPolicy.Advanced, conf.getAllocatorLeakDetectionPolicy());

        conf.setProperty(AbstractConfiguration.ALLOCATOR_LEAK_DETECTION_POLICY, "advanCed");
        assertEquals(LeakDetectionPolicy.Advanced, conf.getAllocatorLeakDetectionPolicy());

        conf.setProperty(AbstractConfiguration.ALLOCATOR_LEAK_DETECTION_POLICY, "paranoiD");
        assertEquals(LeakDetectionPolicy.Paranoid, conf.getAllocatorLeakDetectionPolicy());

        System.getProperties().remove(nettyOldLevelKey);
        System.getProperties().remove(nettyLevelKey);
        //Revert the netty properties.
        if (nettyOldLevelStr != null) {
            System.getProperties().put(nettyOldLevelKey, nettyOldLevelStr);
        }
        if (nettyLevelStr != null) {
            System.getProperties().put(nettyLevelKey, nettyLevelStr);
        }
    }
}
