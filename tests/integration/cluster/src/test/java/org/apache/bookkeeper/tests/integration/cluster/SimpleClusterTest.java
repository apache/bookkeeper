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

package org.apache.bookkeeper.tests.integration.cluster;

import static org.junit.Assert.assertTrue;

import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.junit.Test;

/**
 * A simple test on bookkeeper cluster operations, e.g. start bookies, stop bookies and list bookies.
 */
@Slf4j
public class SimpleClusterTest extends BookKeeperClusterTestBase {

    @Test
    public void getWritableBookiesEmpty() throws Exception {
        Set<BookieSocketAddress> bookies =
            FutureUtils.result(metadataClientDriver.getRegistrationClient().getWritableBookies()).getValue();
        assertTrue(bookies.isEmpty());
    }

}
