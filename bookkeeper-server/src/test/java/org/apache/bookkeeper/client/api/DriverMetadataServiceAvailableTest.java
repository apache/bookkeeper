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
package org.apache.bookkeeper.client.api;

import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.awaitility.Awaitility;
import org.junit.Test;

/**
 * Bookkeeper Client API driver metadata service available test.
 */
public class DriverMetadataServiceAvailableTest extends BookKeeperClusterTestCase {

    public DriverMetadataServiceAvailableTest() {
        super(3);
    }

    @Test
    public void testDriverMetadataServiceAvailable()
            throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        conf.setZkTimeout(3000);
        try (BookKeeper bkc = BookKeeper.newBuilder(conf).build()) {
            Awaitility.await().until(() -> bkc.isDriverMetadataServiceAvailable().get());
            zkUtil.sleepCluster(5, TimeUnit.SECONDS);
            Awaitility.await().until(() -> !bkc.isDriverMetadataServiceAvailable().get());
            Awaitility.await().until(() -> bkc.isDriverMetadataServiceAvailable().get());
        }
    }
}
