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
package org.apache.bookkeeper.client;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 * Test that policies which extended DefaultEnsemblePlacementPolicy of BookKeeper 4.4 work with current version
 * @author enrico.olivelli
 */
public class LegacyCustomDefaultEnsemblePlacementPolicyTest extends BookKeeperClusterTestCase {

    private BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
    private static final String PASSWORD = "testPasswd";
    private static final String property = "foo";
    private static final byte[] value = "bar".getBytes(StandardCharsets.UTF_8);
    private static AtomicBoolean legacyNewEnsembleMethodCalled = new AtomicBoolean(false);
    private static AtomicBoolean legacyReplaceBookieMethodCalled = new AtomicBoolean(false);

    public LegacyCustomDefaultEnsemblePlacementPolicyTest() {
        super(0);
        baseClientConf.setEnsemblePlacementPolicy(Custom44ClientEnsemblePlacementPolicy.class);
    }

    public static final class Custom44ClientEnsemblePlacementPolicy extends DefaultEnsemblePlacementPolicy {

        @Override
        public BookieSocketAddress replaceBookie(BookieSocketAddress bookieToReplace,
            Set<BookieSocketAddress> excludeBookies) throws BKException.BKNotEnoughBookiesException {
            legacyReplaceBookieMethodCalled.set(true);
            return super.replaceBookie(bookieToReplace, excludeBookies);
        }

        @Override
        public ArrayList<BookieSocketAddress> newEnsemble(int ensembleSize, int quorumSize, Set<BookieSocketAddress> excludeBookies) throws BKException.BKNotEnoughBookiesException {
            legacyNewEnsembleMethodCalled.set(true);
            return super.newEnsemble(ensembleSize, quorumSize, excludeBookies);
        }

    }

    @Before
    public void reset() {
        legacyReplaceBookieMethodCalled.set(false);
        legacyNewEnsembleMethodCalled.set(false);
    }

    @Test(timeout = 60000)
    public void testNewEnsemble() throws Exception {
        numBookies = 1;
        startBKCluster();
        try {
            Map<String, byte[]> customMetadata = new HashMap<>();
            customMetadata.put(property, value);
            try (BookKeeper bk = new BookKeeper(baseClientConf, zkc)) {
                bk.createLedger(1, 1, 1, digestType, PASSWORD.getBytes(), customMetadata);
            }
            Assert.assertTrue(legacyNewEnsembleMethodCalled.get());
        } finally {
            stopBKCluster();
        }
    }

    @Test(timeout = 60000)
    public void testReplaceBookie() throws Exception {
        numBookies = 3;
        startBKCluster();
        try {
            Map<String, byte[]> customMetadata = new HashMap<>();
            customMetadata.put(property, value);
            try (BookKeeper bk = new BookKeeper(baseClientConf, zkc)) {
                try (LedgerHandle lh = bk.createLedger(2, 2, 2, digestType, PASSWORD.getBytes(), customMetadata)) {
                    lh.addEntry(value);
                    long lId = lh.getId();
                    ArrayList<BookieSocketAddress> ensembleAtFirstEntry = lh.getLedgerMetadata().getEnsemble(lId);
                    assertEquals(2, ensembleAtFirstEntry.size());
                    killBookie(ensembleAtFirstEntry.get(0));
                    lh.addEntry(value);
                }
            }
            Assert.assertTrue(legacyReplaceBookieMethodCalled.get());
        } finally {
            stopBKCluster();
        }
    }

}
