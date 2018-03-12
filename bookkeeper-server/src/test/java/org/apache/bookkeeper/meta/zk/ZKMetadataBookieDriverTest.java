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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.discover.RegistrationManager.RegistrationListener;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test {@link ZKMetadataBookieDriver}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ZKMetadataDriverBase.class, ZooKeeperClient.class, ZKMetadataBookieDriver.class })
public class ZKMetadataBookieDriverTest extends ZKMetadataDriverTestBase {

    private ZKMetadataBookieDriver driver;
    private ServerConfiguration conf;

    @Before
    public void setup() throws Exception {
        this.conf = new ServerConfiguration();
        super.setup(conf);

        driver = new ZKMetadataBookieDriver();
    }

    @Test
    public void testGetRegManager() throws Exception {
        RegistrationListener listener = mock(RegistrationListener.class);
        driver.initialize(conf, listener, NullStatsLogger.INSTANCE);

        assertSame(conf, driver.serverConf);
        assertSame(listener, driver.listener);
        assertNull(driver.regManager);

        ZKRegistrationManager mockRegManager = PowerMockito.mock(ZKRegistrationManager.class);

        PowerMockito.whenNew(ZKRegistrationManager.class)
            .withParameterTypes(
                ServerConfiguration.class,
                ZooKeeper.class,
                RegistrationListener.class)
            .withArguments(
                any(ServerConfiguration.class),
                any(ZooKeeper.class),
                any(RegistrationListener.class))
            .thenReturn(mockRegManager);

        RegistrationManager manager = driver.getRegistrationManager();
        assertSame(mockRegManager, manager);
        assertSame(mockRegManager, driver.regManager);

        PowerMockito.verifyNew(ZKRegistrationManager.class, times(1))
            .withArguments(
                same(conf),
                same(mockZkc),
                same(listener));

        driver.close();
        verify(mockRegManager, times(1)).close();
        assertNull(driver.regManager);
    }

}
