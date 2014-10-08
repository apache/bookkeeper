package org.apache.bookkeeper.test;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

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

import org.apache.bookkeeper.meta.LedgerManagerFactory;

/**
 * Test Case run over different ledger manager.
 */
@RunWith(Parameterized.class)
public abstract class MultiLedgerManagerTestCase extends BookKeeperClusterTestCase {

    public MultiLedgerManagerTestCase(int numBookies) {
        super(numBookies);
    }

    @Parameters
    public static Collection<Object[]> configs() {
        String[] ledgerManagers = new String[] {
            "org.apache.bookkeeper.meta.FlatLedgerManagerFactory",
            "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory",
            "org.apache.bookkeeper.meta.MSLedgerManagerFactory",
        };
        ArrayList<Object[]> cfgs = new ArrayList<Object[]>(ledgerManagers.length);
        for (String lm : ledgerManagers) {
            cfgs.add(new Object[] { lm });
        }
        return cfgs;
    }
}
