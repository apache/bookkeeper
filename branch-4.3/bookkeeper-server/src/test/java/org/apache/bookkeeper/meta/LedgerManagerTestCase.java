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

package org.apache.bookkeeper.meta;

import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.SnapshotMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test case to run over serveral ledger managers
 */
@RunWith(Parameterized.class)
public abstract class LedgerManagerTestCase extends BookKeeperClusterTestCase {
    static final Logger LOG = LoggerFactory.getLogger(LedgerManagerTestCase.class);

    LedgerManagerFactory ledgerManagerFactory;
    LedgerManager ledgerManager = null;
    SnapshotMap<Long, Boolean> activeLedgers = null;

    public LedgerManagerTestCase(Class<? extends LedgerManagerFactory> lmFactoryCls) {
        super(0);
        activeLedgers = new SnapshotMap<Long, Boolean>();
        baseConf.setLedgerManagerFactoryClass(lmFactoryCls);
    }

    public LedgerManager getLedgerManager() {
        if (null == ledgerManager) {
            ledgerManager = ledgerManagerFactory.newLedgerManager();
        }
        return ledgerManager;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
            { FlatLedgerManagerFactory.class },
            { HierarchicalLedgerManagerFactory.class },
            { MSLedgerManagerFactory.class }
        });
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        ledgerManagerFactory = LedgerManagerFactory.newLedgerManagerFactory(baseConf, zkc);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (null != ledgerManager) {
            ledgerManager.close();
        }
        ledgerManagerFactory.uninitialize();
        super.tearDown();
    }

}
