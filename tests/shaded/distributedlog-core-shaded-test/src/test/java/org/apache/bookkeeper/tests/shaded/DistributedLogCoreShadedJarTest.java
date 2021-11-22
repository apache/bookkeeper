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
package org.apache.bookkeeper.tests.shaded;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


import dlshade.org.apache.bookkeeper.common.util.ReflectionUtils;
import dlshade.org.apache.bookkeeper.conf.AbstractConfiguration;
import dlshade.org.apache.bookkeeper.conf.ServerConfiguration;
import dlshade.org.apache.bookkeeper.meta.AbstractZkLedgerManagerFactory;
import dlshade.org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import dlshade.org.apache.bookkeeper.meta.LayoutManager;
import dlshade.org.apache.bookkeeper.meta.LedgerLayout;
import dlshade.org.apache.bookkeeper.meta.LedgerManagerFactory;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test whether the distributedlog-core-shaded jar is generated correctly.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ AbstractZkLedgerManagerFactory.class, ReflectionUtils.class })
public class DistributedLogCoreShadedJarTest {

    @Test(expected = ClassNotFoundException.class)
    public void testProtobufIsShaded() throws Exception {
        Class.forName("com.google.protobuf.Message");
    }

    @Test
    public void testProtobufShadedPath() throws Exception {
        Class.forName("dlshade.com.google.protobuf.Message");
    }

    @Test(expected = ClassNotFoundException.class)
    public void testGuavaIsShaded() throws Exception {
        Class.forName("com.google.common.cache.Cache");
    }

    @Test
    public void testGuavaShadedPath() throws Exception {
        Class.forName("dlshade.com.google.common.cache.Cache");
        assertTrue(true);
    }

    @Test(expected = ClassNotFoundException.class)
    public void testZooKeeperIsShaded() throws Exception {
        Class.forName("org.apache.zookeeper.ZooKeeper");
    }

    @Test
    public void testZooKeeperShadedPath() throws Exception {
        Class.forName("dlshade.org.apache.zookeeper.ZooKeeper");
    }

    @Test(expected = ClassNotFoundException.class)
    public void testBookKeeperCommon() throws Exception {
        Class.forName("org.apache.bookkeeper.common.util.OrderedExecutor");
        assertTrue(true);
    }

    @Test
    public void testBookKeeperCommonShade() throws Exception {
        Class.forName("dlshade.org.apache.bookkeeper.common.util.OrderedExecutor");
        assertTrue(true);
    }

    @Test(expected = ClassNotFoundException.class)
    public void testBookKeeperProto() throws Exception {
        Class.forName("org.apache.bookkeeper.proto.BookkeeperProtocol");
    }

    @Test
    public void testBookKeeperProtoShade() throws Exception {
        Class.forName("dlshade.org.apache.bookkeeper.proto.BookkeeperProtocol");
        assertTrue(true);
    }

    @Test(expected = ClassNotFoundException.class)
    public void testCirceChecksum() throws Exception {
        Class.forName("com.scurrilous.circe.checksum.Crc32cIntChecksum");
    }

    @Test
    public void testCirceChecksumShade() throws Exception {
        Class.forName("dlshade.com.scurrilous.circe.checksum.Crc32cIntChecksum");
        assertTrue(true);
    }

    @Test
    public void testDistributedLogCommon() throws Exception {
        Class.forName("dlshade.org.apache.distributedlog.common.concurrent.AsyncSemaphore");
        assertTrue(true);
    }

    @Test
    public void testDistributedLogProto() throws Exception {
        Class.forName("dlshade.org.apache.distributedlog.DLSN");
        assertTrue(true);
    }

    @Test
    public void testDistributedLogCore() throws Exception {
        Class.forName("dlshade.org.apache.distributedlog.api.AsyncLogReader");
        assertTrue(true);
    }

    @Test
    public void testShadeLedgerManagerFactoryWithoutConfiguredLedgerManagerClass() throws Exception {
        testShadeLedgerManagerFactoryAllowed(
            null,
            true);
    }


    @Test
    public void testShadeLedgerManagerFactoryWithConfiguredLedgerManagerClass() throws Exception {
        testShadeLedgerManagerFactoryAllowed(
            "org.apache.bookkeeper.meta.HirerchicalLedgerManagerFactory",
            true);
    }

    @Test
    public void testShadeLedgerManagerFactoryDisallowedWithoutConfiguredLedgerManagerClass() throws Exception {
        testShadeLedgerManagerFactoryAllowed(
            null,
            false);
    }


    @Test
    public void testShadeLedgerManagerFactoryDisallowedWithConfiguredLedgerManagerClass() throws Exception {
        testShadeLedgerManagerFactoryAllowed(
            "org.apache.bookkeeper.meta.HirerchicalLedgerManagerFactory",
            false);
    }

    @SuppressWarnings("unchecked")
    private void testShadeLedgerManagerFactoryAllowed(String factoryClassName,
                                                      boolean allowShaded) throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setAllowShadedLedgerManagerFactoryClass(allowShaded);
        conf.setLedgerManagerFactoryClassName(factoryClassName);

        LayoutManager manager = mock(LayoutManager.class);
        LedgerLayout layout = new LedgerLayout(
            "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory",
            HierarchicalLedgerManagerFactory.CUR_VERSION);
        when(manager.readLedgerLayout()).thenReturn(layout);

        LedgerManagerFactory factory = mock(LedgerManagerFactory.class);
        when(factory.initialize(any(AbstractConfiguration.class), same(manager), anyInt()))
            .thenReturn(factory);
        PowerMockito.mockStatic(ReflectionUtils.class);
        when(ReflectionUtils.newInstance(any(Class.class)))
            .thenReturn(factory);

        try {
            LedgerManagerFactory result = AbstractZkLedgerManagerFactory.newLedgerManagerFactory(
                conf, manager);
            if (allowShaded) {
                assertSame(factory, result);
                verify(factory, times(1))
                    .initialize(any(AbstractConfiguration.class), same(manager), anyInt());
            } else {
                fail("Should fail to instantiate ledger manager factory if allowShaded is false");
            }
        } catch (IOException ioe) {
            if (allowShaded) {
                fail("Should not fail to instantiate ledger manager factory is allowShaded is true");
            } else {
                assertTrue(ioe.getCause() instanceof ClassNotFoundException);
            }
        }
    }
}
