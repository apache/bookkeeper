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

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import junit.framework.TestCase;

/**
 * Test case to run over serveral ledger managers
 */
@RunWith(Parameterized.class)
public abstract class LedgerManagerTestCase extends BookKeeperClusterTestCase {
    static final Logger LOG = LoggerFactory.getLogger(LedgerManagerTestCase.class);

    LedgerManager ledgerManager;

    public LedgerManagerTestCase(String ledgerManagerType) {
        super(0);
        baseConf.setLedgerManagerType(ledgerManagerType);
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
            { FlatLedgerManager.NAME },
            { HierarchicalLedgerManager.NAME }
        });
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        ledgerManager = LedgerManagerFactory.newLedgerManager(baseConf, zkc);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        ledgerManager.close();
        super.tearDown();
    }

}
