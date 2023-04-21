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
package org.apache.bookkeeper.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

/**
 * Unit test of {@link LedgerLayout} class itself.
 */
public class TestLedgerLayout {

    private static final LedgerLayout hierarchical1 =
        new LedgerLayout(
            HierarchicalLedgerManagerFactory.class.getName(),
            1);

    private static final LedgerLayout hierarchical2 =
        new LedgerLayout(
            HierarchicalLedgerManagerFactory.class.getName(),
            2);

    private static final LedgerLayout longHierarchical =
        new LedgerLayout(
            LongHierarchicalLedgerManagerFactory.class.getName(),
            1);

    @Test
    public void testEquals() {
        assertEquals(hierarchical1, hierarchical1);
        assertNotEquals(hierarchical1, hierarchical2);
        assertNotEquals(hierarchical1, longHierarchical);
    }

    @Test
    public void testGetters() {
        assertEquals(
            HierarchicalLedgerManagerFactory.class.getName(),
            hierarchical1.getManagerFactoryClass());
        assertEquals(
            1,
            hierarchical1.getManagerVersion());
        assertEquals(
            LedgerLayout.LAYOUT_FORMAT_VERSION,
            hierarchical1.getLayoutFormatVersion());
    }

}
