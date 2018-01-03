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
package org.apache.bookkeeper.discover;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerLayout;
import org.junit.Test;

/**
 * Unit test of {@link RegistrationManager}.
 */
public class TestZkRegistrationManager {
    private final LedgerLayout ledgerLayout;
    private final LayoutManager layoutManager;
    private final ZKRegistrationManager zkRegistrationManager;

    public TestZkRegistrationManager() {
        this.ledgerLayout = mock(LedgerLayout.class);
        this.layoutManager = mock(LayoutManager.class);
        this.zkRegistrationManager = new ZKRegistrationManager();
        zkRegistrationManager.setLayoutManager(layoutManager);
    }

    @Test
    public void testGetLayoutManager() throws Exception {
        assertEquals(layoutManager, zkRegistrationManager.getLayoutManager());
    }

    @Test
    public void testReadLedgerLayout() throws Exception {
        when(layoutManager.readLedgerLayout()).thenReturn(ledgerLayout);
        assertEquals(ledgerLayout, zkRegistrationManager.readLedgerLayout());
    }

    @Test
    public void testStoreLedgerLayout() throws Exception {
        zkRegistrationManager.storeLedgerLayout(ledgerLayout);

        verify(layoutManager, times(1))
            .storeLedgerLayout(eq(ledgerLayout));
    }

    @Test
    public void testDeleteLedgerLayout() throws Exception {
        zkRegistrationManager.deleteLedgerLayout();

        verify(layoutManager, times(1))
            .deleteLedgerLayout();
    }
}
