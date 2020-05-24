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

package org.apache.bookkeeper.conf;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

/**
 * Unit test for load bookkeeper configuration from environment {@link AbstractConfiguration}.
 */
public class TestServerConfigurationWithEnvironment {

    private final ServerConfiguration serverConf;

    public TestServerConfigurationWithEnvironment() {
        serverConf = new ServerConfiguration();
    }

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Before
    public void setup() throws Exception {
        environmentVariables.set("BK_bookiePort", "3182");
        environmentVariables.set("BK_allowLoopback", "true");
        environmentVariables.set("BK_bookieDeathWatchInterval", "1000");
        environmentVariables.set("BK_extraServerComponents", "test1,test2,test3");
        environmentVariables.set("BK_diskUsageWarnThreshold", "0.95");
        environmentVariables.set("BK_minorCompactionThreshold", "0.2");
        environmentVariables.set("BK_gcWaitTime", "1000");
        serverConf.loadEnv();
    }

    @Test
    public void testEnvironmentVariables() throws ConfigurationException {
        serverConf.validate();
        assertEquals(3182, serverConf.getBookiePort());
        assertEquals(true, serverConf.getAllowLoopback());
        assertEquals(1000, serverConf.getDeathWatchInterval());
        assertEquals(0.95, serverConf.getDiskUsageWarnThreshold(), 0.2);
        assertEquals(0.2, serverConf.getMinorCompactionThreshold(), 0.1);
        assertEquals(1000L, serverConf.getGcWaitTime());
        assertEquals(10000L, serverConf.getFlushInterval());
        assertArrayEquals(new String[]{"test1", "test2", "test3"},
            serverConf.getExtraServerComponents());
    }
}
