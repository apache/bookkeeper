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
package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.tools.cli.commands.bookies.ClusterInfoCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.junit.Test;

/**
 * Integration test of {@link org.apache.bookkeeper.tools.cli.commands.bookies.ClusterInfoCommand}.
 */
public class ClusterInfoCommandTest extends BookKeeperClusterTestCase {

    public ClusterInfoCommandTest() {
        super(1);
    }

    @Test
    public void testClusterInfo() throws Exception {
        ClusterInfoCommand clusterInfoCommand = new ClusterInfoCommand();
        final ServerConfiguration conf = confByIndex(0);

        assertNull(clusterInfoCommand.info());

        clusterInfoCommand.apply(conf, new CliFlags());

        assertNotNull(clusterInfoCommand.info());
        ClusterInfoCommand.ClusterInfo info = clusterInfoCommand.info();
        assertEquals(1, info.getTotalBookiesCount());
        assertEquals(1, info.getWritableBookiesCount());
        assertEquals(0, info.getReadonlyBookiesCount());
        assertEquals(0, info.getUnavailableBookiesCount());
        assertFalse(info.isAuditorElected());
        assertEquals("", info.getAuditorId());
        assertFalse(info.isClusterUnderReplicated());
        assertTrue(info.isLedgerReplicationEnabled());
    }

}
