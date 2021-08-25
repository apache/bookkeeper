/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.conf;

import static org.junit.Assert.assertEquals;

import java.util.NoSuchElementException;
import org.junit.Test;

/**
 * Test Configuration API.
 *
 * @see SystemPropertiesConfigurationTest
 */
public class NoSystemPropertiesConfigurationTest {

    static {
        // this property is read when AbstractConfiguration class is loaded.
        // this test will work as expected only using a new JVM (or classloader) for the test
        System.setProperty(ClientConfiguration.THROTTLE, "10");
        System.setProperty(ClientConfiguration.CLIENT_TCP_USER_TIMEOUT_MILLIS, "20000");
    }

    @Test(expected = NoSuchElementException.class)
    public void testUseSystemProperty() {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        assertEquals(5000, clientConfiguration.getThrottleValue());
        // This should throw NoSuchElementException if the property has not been set.
        clientConfiguration.getTcpUserTimeoutMillis();
    }
}
