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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.tests.containers.BookieContainer;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * A simple test on bookkeeper cluster operations, e.g. start bookies, stop bookies and list bookies.
 */
@Slf4j
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SimpleClusterTest extends BookKeeperClusterTestBase {

    @Test
    public void test000_ClusterIsEmpty() throws Exception {
        Set<BookieId> bookies =
            FutureUtils.result(metadataClientDriver.getRegistrationClient().getWritableBookies()).getValue();
        assertTrue(bookies.isEmpty());
    }

    @Test
    public void test001_StartBookie() throws Exception {
        String bookieName = "bookie-000";
        BookieContainer container = bkCluster.createBookie(bookieName);
        assertNotNull("Container should be started", container);
        assertEquals(1, bkCluster.getBookieContainers().size());
        assertSame(container, bkCluster.getBookie(bookieName));

        Set<BookieId> bookies =
            FutureUtils.result(metadataClientDriver.getRegistrationClient().getWritableBookies()).getValue();
        assertEquals(1, bookies.size());
    }

    @Test
    public void test002_StopBookie() throws Exception {
        String bookieName = "bookie-000";
        BookieContainer container = bkCluster.killBookie(bookieName);
        assertNotNull("Bookie '" + bookieName + "' doesn't exist", container);
        assertEquals(0, bkCluster.getBookieContainers().size());
        assertNull(bkCluster.getBookie(bookieName));

        waitUntilBookieUnregistered(bookieName);

        Set<BookieId> bookies =
            FutureUtils.result(metadataClientDriver.getRegistrationClient().getWritableBookies()).getValue();
        assertEquals(0, bookies.size());
    }

}
