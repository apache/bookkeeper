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
package org.apache.bookkeeper.meta;

import static org.apache.bookkeeper.meta.MetadataDrivers.BK_METADATA_BOOKIE_DRIVERS_PROPERTY;
import static org.apache.bookkeeper.meta.MetadataDrivers.BK_METADATA_CLIENT_DRIVERS_PROPERTY;
import static org.apache.bookkeeper.meta.MetadataDrivers.ZK_BOOKIE_DRIVER_CLASS;
import static org.apache.bookkeeper.meta.MetadataDrivers.ZK_CLIENT_DRIVER_CLASS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Maps;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataDrivers.MetadataBookieDriverInfo;
import org.apache.bookkeeper.meta.MetadataDrivers.MetadataClientDriverInfo;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link MetadataDrivers}.
 */
public class MetadataDriversTest {

    abstract static class TestClientDriver implements MetadataClientDriver {

        @Override
        public MetadataClientDriver initialize(ClientConfiguration conf,
                                               ScheduledExecutorService scheduler,
                                               StatsLogger statsLogger,
                                               Optional<Object> ctx) throws MetadataException {
            return this;
        }

        @Override
        public RegistrationClient getRegistrationClient() {
            return mock(RegistrationClient.class);
        }

        @Override
        public LedgerManagerFactory getLedgerManagerFactory() throws MetadataException {
            return mock(LedgerManagerFactory.class);
        }

        @Override
        public LayoutManager getLayoutManager() {
            return mock(LayoutManager.class);
        }

        @Override
        public void close() {
        }

        @Override
        public void setSessionStateListener(SessionStateListener sessionStateListener) {
        }
    }

    static class ClientDriver1 extends TestClientDriver {

        @Override
        public String getScheme() {
            return "driver1";
        }


    }

    static class ClientDriver2 extends TestClientDriver {

        @Override
        public String getScheme() {
            return "driver2";
        }

    }

    abstract static class TestBookieDriver implements MetadataBookieDriver {
        @Override
        public MetadataBookieDriver initialize(ServerConfiguration conf,
                                               StatsLogger statsLogger) throws MetadataException {
            return this;
        }

        @Override
        public RegistrationManager createRegistrationManager() {
            return mock(RegistrationManager.class);
        }

        @Override
        public LedgerManagerFactory getLedgerManagerFactory() throws MetadataException {
            return mock(LedgerManagerFactory.class);
        }

        @Override
        public LayoutManager getLayoutManager() {
            return mock(LayoutManager.class);
        }

        @Override
        public void close() {

        }
    }

    static class BookieDriver1 extends TestBookieDriver {

        @Override
        public String getScheme() {
            return "driver1";
        }

    }

    static class BookieDriver2 extends TestBookieDriver {

        @Override
        public String getScheme() {
            return "driver2";
        }

    }

    private Map<String, MetadataClientDriverInfo> savedClientDrivers;
    private Map<String, MetadataBookieDriverInfo> savedBookieDrivers;

    @Before
    public void setup() {
        savedClientDrivers = Maps.newHashMap(MetadataDrivers.getClientDrivers());
        savedBookieDrivers = Maps.newHashMap(MetadataDrivers.getBookieDrivers());
    }

    @After
    public void teardown() {
        MetadataDrivers.getClientDrivers().clear();
        MetadataDrivers.getClientDrivers().putAll(savedClientDrivers);
        MetadataDrivers.getBookieDrivers().clear();
        MetadataDrivers.getBookieDrivers().putAll(savedBookieDrivers);
    }

    @Test
    public void testDefaultDrivers() {
        MetadataClientDriver clientDriver = MetadataDrivers.getClientDriver("zk");
        assertEquals(
            ZK_CLIENT_DRIVER_CLASS,
            clientDriver.getClass().getName());
        clientDriver = MetadataDrivers.getClientDriver(URI.create("zk+hierarchical://127.0.0.1/ledgers"));
        assertEquals(
            ZK_CLIENT_DRIVER_CLASS,
            clientDriver.getClass().getName());

        MetadataBookieDriver bookieDriver = MetadataDrivers.getBookieDriver("zk");
        assertEquals(
            ZK_BOOKIE_DRIVER_CLASS,
            bookieDriver.getClass().getName());
        bookieDriver = MetadataDrivers.getBookieDriver(URI.create("zk+hierarchical://127.0.0.1/ledgers"));
        assertEquals(
            ZK_BOOKIE_DRIVER_CLASS,
            bookieDriver.getClass().getName());
    }

    @Test(expected = NullPointerException.class)
    public void testClientDriverNullScheme() {
        MetadataDrivers.getClientDriver((String) null);
    }

    @Test(expected = NullPointerException.class)
    public void testBookieDriverNullScheme() {
        MetadataDrivers.getBookieDriver((String) null);
    }

    @Test(expected = NullPointerException.class)
    public void testClientDriverNullURI() {
        MetadataDrivers.getClientDriver((URI) null);
    }

    @Test(expected = NullPointerException.class)
    public void testBookieDriverNullURI() {
        MetadataDrivers.getBookieDriver((URI) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testClientDriverUnknownScheme() {
        MetadataDrivers.getClientDriver("unknown");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBookieDriverUnknownScheme() {
        MetadataDrivers.getBookieDriver("unknown");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testClientDriverUnknownSchemeURI() {
        MetadataDrivers.getClientDriver(URI.create("unknown://"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBookieDriverUnknownSchemeURI() {
        MetadataDrivers.getBookieDriver(URI.create("unknown://"));
    }

    @Test(expected = NullPointerException.class)
    public void testClientDriverNullSchemeURI() {
        MetadataDrivers.getClientDriver(URI.create("//127.0.0.1/ledgers"));
    }

    @Test(expected = NullPointerException.class)
    public void testBookieDriverNullSchemeURI() {
        MetadataDrivers.getBookieDriver(URI.create("//127.0.0.1/ledgers"));
    }

    @Test
    public void testClientDriverLowerUpperCasedSchemes() {
        String[] schemes = new String[] {
            "zk", "Zk", "zK", "ZK"
        };
        for (String scheme : schemes) {
            MetadataClientDriver clientDriver = MetadataDrivers.getClientDriver(scheme);
            assertEquals(
                ZK_CLIENT_DRIVER_CLASS,
                clientDriver.getClass().getName());
        }
    }

    @Test
    public void testBookieDriverLowerUpperCasedSchemes() {
        String[] schemes = new String[] {
            "zk", "Zk", "zK", "ZK"
        };
        for (String scheme : schemes) {
            MetadataBookieDriver bookieDriver = MetadataDrivers.getBookieDriver(scheme);
            assertEquals(
                ZK_BOOKIE_DRIVER_CLASS,
                bookieDriver.getClass().getName());
        }
    }

    @Test
    public void testRegisterClientDriver() throws Exception {
        MetadataClientDriver clientDriver = mock(MetadataClientDriver.class);
        when(clientDriver.getScheme()).thenReturn("testdriver");

        try {
            MetadataDrivers.getClientDriver(clientDriver.getScheme());
            fail("Should fail to get client driver if it is not registered");
        } catch (IllegalArgumentException iae) {
            // expected
        }

        MetadataDrivers.registerClientDriver(clientDriver.getScheme(), clientDriver.getClass());
        MetadataClientDriver driver = MetadataDrivers.getClientDriver(clientDriver.getScheme());
        assertEquals(clientDriver.getClass(), driver.getClass());
    }

    @Test
    public void testRegisterBookieDriver() throws Exception {
        MetadataBookieDriver bookieDriver = mock(MetadataBookieDriver.class);
        when(bookieDriver.getScheme()).thenReturn("testdriver");

        try {
            MetadataDrivers.getBookieDriver(bookieDriver.getScheme());
            fail("Should fail to get bookie driver if it is not registered");
        } catch (IllegalArgumentException iae) {
            // expected
        }

        MetadataDrivers.registerBookieDriver(bookieDriver.getScheme(), bookieDriver.getClass());
        MetadataBookieDriver driver = MetadataDrivers.getBookieDriver(bookieDriver.getScheme());
        assertEquals(bookieDriver.getClass(), driver.getClass());
    }

    @Test
    public void testLoadClientDriverFromSystemProperty() throws Exception {
        String saveDriversStr = System.getProperty(BK_METADATA_CLIENT_DRIVERS_PROPERTY);
        try {
            System.setProperty(
                BK_METADATA_CLIENT_DRIVERS_PROPERTY,
                StringUtils.join(new String[] {
                    ClientDriver1.class.getName(),
                    ClientDriver2.class.getName()
                }, ':'));

            MetadataDrivers.loadInitialDrivers();

            MetadataClientDriver loadedDriver1 = MetadataDrivers.getClientDriver("driver1");
            assertEquals(ClientDriver1.class, loadedDriver1.getClass());
            MetadataClientDriver loadedDriver2 = MetadataDrivers.getClientDriver("driver2");
            assertEquals(ClientDriver2.class, loadedDriver2.getClass());
        } finally {
            if (null != saveDriversStr) {
                System.setProperty(BK_METADATA_CLIENT_DRIVERS_PROPERTY, saveDriversStr);
            } else {
                System.clearProperty(BK_METADATA_CLIENT_DRIVERS_PROPERTY);
            }
        }
    }

    @Test
    public void testLoadBookieDriverFromSystemProperty() throws Exception {
        String saveDriversStr = System.getProperty(BK_METADATA_BOOKIE_DRIVERS_PROPERTY);
        try {
            System.setProperty(
                BK_METADATA_BOOKIE_DRIVERS_PROPERTY,
                StringUtils.join(new String[] {
                    BookieDriver1.class.getName(),
                    BookieDriver2.class.getName()
                }, ':'));

            MetadataDrivers.loadInitialDrivers();

            MetadataBookieDriver loadedDriver1 = MetadataDrivers.getBookieDriver("driver1");
            assertEquals(BookieDriver1.class, loadedDriver1.getClass());
            MetadataBookieDriver loadedDriver2 = MetadataDrivers.getBookieDriver("driver2");
            assertEquals(BookieDriver2.class, loadedDriver2.getClass());
        } finally {
            if (null != saveDriversStr) {
                System.setProperty(BK_METADATA_BOOKIE_DRIVERS_PROPERTY, saveDriversStr);
            } else {
                System.clearProperty(BK_METADATA_BOOKIE_DRIVERS_PROPERTY);
            }
        }
    }

}
