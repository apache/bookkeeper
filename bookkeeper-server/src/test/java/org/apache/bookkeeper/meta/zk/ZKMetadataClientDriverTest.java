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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.discover.ZKRegistrationClient;
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
 * Unit test {@link ZKMetadataClientDriver}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ZKMetadataDriverBase.class, ZooKeeperClient.class, ZKMetadataClientDriver.class })
public class ZKMetadataClientDriverTest extends ZKMetadataDriverTestBase {

    private ZKMetadataClientDriver driver;
    private ClientConfiguration conf;

    @Before
    public void setup() throws Exception {
        this.conf = new ClientConfiguration();
        super.setup(conf);

        driver = new ZKMetadataClientDriver();
    }

    @Test
    public void testGetRegClient() throws Exception {
        ScheduledExecutorService mockExecutor = mock(ScheduledExecutorService.class);
        driver.initialize(conf, mockExecutor, NullStatsLogger.INSTANCE, Optional.empty());

        assertSame(conf, driver.clientConf);
        assertSame(mockExecutor, driver.scheduler);
        assertNull(driver.regClient);

        ZKRegistrationClient mockRegClient = PowerMockito.mock(ZKRegistrationClient.class);

        PowerMockito.whenNew(ZKRegistrationClient.class)
            .withParameterTypes(ZooKeeper.class, String.class, ScheduledExecutorService.class, Boolean.TYPE)
            .withArguments(any(ZooKeeper.class), anyString(), any(ScheduledExecutorService.class), anyBoolean())
            .thenReturn(mockRegClient);

        RegistrationClient client = driver.getRegistrationClient();
        assertSame(mockRegClient, client);
        assertSame(mockRegClient, driver.regClient);

        PowerMockito.verifyNew(ZKRegistrationClient.class, times(1))
            .withArguments(eq(mockZkc), eq(ledgersRootPath), eq(mockExecutor), anyBoolean());

        driver.close();
        verify(mockRegClient, times(1)).close();
        assertNull(driver.regClient);
    }

}
