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
package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.junit.Before;

/**
 * Tests for bckpressure handling on the server side with V2 protocol.
 */
public class BookieBackpressureForV2Test extends BookieBackpressureTest {

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseClientConf.setUseV2WireProtocol(true);
        bkc = new BookKeeperTestClient(baseClientConf, new TestStatsProvider());

        // the backpressure will bloc the read response, disable it to let it use backpressure mechanism
        confByIndex(0).setReadWorkerThreadsThrottlingEnabled(false);
    }
}
