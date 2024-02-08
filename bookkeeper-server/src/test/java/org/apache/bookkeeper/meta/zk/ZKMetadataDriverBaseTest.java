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
package org.apache.bookkeeper.meta.zk;

import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

import java.util.Optional;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.AbstractZkLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test of {@link ZKMetadataDriverBase}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ZKMetadataDriverBase.class, ZooKeeperClient.class, AbstractZkLedgerManagerFactory.class })
public class ZKMetadataDriverBaseTest extends ZKMetadataDriverTestBase {

    private ZKMetadataDriverBase driver;
    private RetryPolicy retryPolicy;

    @Before
    public void setup() throws Exception {
        super.setup(new ClientConfiguration());
        driver = mock(ZKMetadataDriverBase.class, CALLS_REAL_METHODS);
        retryPolicy = mock(RetryPolicy.class);
    }

    @Test
    public void testInitialize() throws Exception {
        driver.initialize(
            conf, NullStatsLogger.INSTANCE, retryPolicy, Optional.empty());

        assertEquals(
            "/path/to/ledgers",
            driver.ledgersRootPath);
        assertTrue(driver.ownZKHandle);

        String readonlyPath = "/path/to/ledgers/" + AVAILABLE_NODE + "/" + READONLY;
        assertSame(mockZkc, driver.zk);
        verifyStatic(ZooKeeperClient.class, times(1));
        ZooKeeperClient.newBuilder();
        verify(mockZkBuilder, times(1)).build();
        verify(mockZkc, times(1))
            .exists(eq(readonlyPath), eq(false));
        assertNotNull(driver.layoutManager);
        assertNull(driver.lmFactory);

        driver.close();

        verify(mockZkc, times(1)).close(5000);
        assertNull(driver.zk);
    }

    @Test
    public void testInitializeExternalZooKeeper() throws Exception {
        ZooKeeperClient anotherZk = mock(ZooKeeperClient.class);

        driver.initialize(
            conf, NullStatsLogger.INSTANCE, retryPolicy, Optional.of(anotherZk));

        assertEquals(
            "/ledgers",
            driver.ledgersRootPath);
        assertFalse(driver.ownZKHandle);

        String readonlyPath = "/path/to/ledgers/" + AVAILABLE_NODE;
        assertSame(anotherZk, driver.zk);
        verifyStatic(ZooKeeperClient.class, times(0));
        ZooKeeperClient.newBuilder();
        verify(mockZkBuilder, times(0)).build();
        verify(mockZkc, times(0))
            .exists(eq(readonlyPath), eq(false));
        assertNotNull(driver.layoutManager);
        assertNull(driver.lmFactory);

        driver.close();

        verify(mockZkc, times(0)).close();
        assertNotNull(driver.zk);
    }

    @Test
    public void testGetLedgerManagerFactory() throws Exception {
        driver.initialize(
            conf, NullStatsLogger.INSTANCE, retryPolicy, Optional.empty());

        mockStatic(AbstractZkLedgerManagerFactory.class);
        LedgerManagerFactory factory = mock(LedgerManagerFactory.class);
        PowerMockito.when(
            AbstractZkLedgerManagerFactory.class,
            "newLedgerManagerFactory",
            same(conf),
            same(driver.layoutManager))
            .thenReturn(factory);

        assertSame(factory, driver.getLedgerManagerFactory());
        assertSame(factory, driver.lmFactory);
        verifyStatic(AbstractZkLedgerManagerFactory.class, times(1));
        AbstractZkLedgerManagerFactory.newLedgerManagerFactory(
            same(conf),
            same(driver.layoutManager));

        driver.close();
        verify(factory, times(1)).close();
        assertNull(driver.lmFactory);
    }

}
