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

import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Unit test {@link ZKMetadataBookieDriver}.
 */
@RunWith(MockitoJUnitRunner.class)
public class ZKMetadataBookieDriverTest extends ZKMetadataDriverTestBase {

    private ZKMetadataBookieDriver driver;
    private ServerConfiguration conf;

    @Before
    public void setup() throws Exception {
        this.conf = new ServerConfiguration();
        super.setup(conf);

        driver = spy(new ZKMetadataBookieDriver());
    }

    @After
    public void teardown() {
        super.teardown();
        driver.close();
    }

    @Test
    public void testGetRegManager() throws Exception {
        driver.initialize(conf, NullStatsLogger.INSTANCE);

        assertSame(conf, driver.serverConf);

        ZKRegistrationManager mockRegManager = mock(ZKRegistrationManager.class);
        doReturn(mockRegManager).when(driver).newZKRegistrationManager(any(ServerConfiguration.class),
                any(ZooKeeper.class));

        try (RegistrationManager manager = driver.createRegistrationManager()) {
            assertSame(mockRegManager, manager);

            verify(driver, times(1)).newZKRegistrationManager(same(conf), same(mockZkc));
        }
    }

}
