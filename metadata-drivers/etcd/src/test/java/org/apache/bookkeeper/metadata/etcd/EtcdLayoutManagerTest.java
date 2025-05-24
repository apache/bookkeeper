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

package org.apache.bookkeeper.metadata.etcd;

import static org.apache.bookkeeper.metadata.etcd.EtcdConstants.LAYOUT_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.meta.LayoutManager.LedgerLayoutExistsException;
import org.apache.bookkeeper.meta.LedgerLayout;
import org.apache.bookkeeper.metadata.etcd.testing.EtcdTestBase;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration test {@link EtcdLayoutManager}.
 */
@Slf4j
public class EtcdLayoutManagerTest extends EtcdTestBase {

    private static final int managerVersion = 0xabcd;

    private String scope;
    private EtcdLayoutManager layoutManager;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.scope = "/" + RandomStringUtils.randomAlphabetic(8);
        this.layoutManager = new EtcdLayoutManager(etcdClient, scope);
        log.info("setup layout manager under scope {}", scope);
    }

    @Test
    public void testReadCreateDeleteLayout() throws Exception {
        // layout doesn't exist
        assertNull(layoutManager.readLedgerLayout());

        // create the layout
        LedgerLayout layout = new LedgerLayout(
            EtcdLedgerManagerFactory.class.getName(),
            managerVersion
        );
        layoutManager.storeLedgerLayout(layout);

        // read the layout
        LedgerLayout readLayout = layoutManager.readLedgerLayout();
        assertEquals(layout, readLayout);

        // attempts to create the layout again and it should fail
        LedgerLayout newLayout = new LedgerLayout(
            "new layout",
            managerVersion + 1
        );
        try {
            layoutManager.storeLedgerLayout(newLayout);
            fail("Should fail storeLedgerLayout if layout already exists");
        } catch (LedgerLayoutExistsException e) {
            // expected
        }

        // read the layout again (layout should not be changed)
        readLayout = layoutManager.readLedgerLayout();
        assertEquals(layout, readLayout);

        // delete the layout
        layoutManager.deleteLedgerLayout();

        // the layout should be gone now
        assertNull(layoutManager.readLedgerLayout());

        // delete the layout again. it should fail since layout doesn't exist
        try {
            layoutManager.deleteLedgerLayout();
            fail("Should fail deleteLedgerLayout is layout not found");
        } catch (IOException ioe) {
            assertEquals(
                "No ledger layout is found under '" + scope + "/" + LAYOUT_NODE + "'",
                ioe.getMessage());
        }
    }

}
