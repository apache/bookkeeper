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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Testing a generic ensemble placement policy.
 */
@RunWith(Parameterized.class)
public class GenericEnsemblePlacementPolicyTest extends BookKeeperClusterTestCase {

    private BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
    private static final String PASSWORD = "testPasswd";
    private static final String property = "foo";
    private static final byte[] value = "bar".getBytes(StandardCharsets.UTF_8);
    private static List<Map<String, byte[]>> customMetadataOnNewEnsembleStack = new ArrayList<>();
    private static List<Map<String, byte[]>> customMetadataOnReplaceBookieStack = new ArrayList<>();

    @Parameters
    public static Collection<Object[]> getDiskWeightBasedPlacementEnabled() {
        return Arrays.asList(new Object[][] { { false }, { true } });
    }

    public GenericEnsemblePlacementPolicyTest(boolean diskWeightBasedPlacementEnabled) {
        super(0);
        baseClientConf.setEnsemblePlacementPolicy(CustomEnsemblePlacementPolicy.class);
        baseClientConf.setDiskWeightBasedPlacementEnabled(diskWeightBasedPlacementEnabled);
    }

    /**
     * A custom ensemble placement policy.
     */
    public static final class CustomEnsemblePlacementPolicy extends DefaultEnsemblePlacementPolicy {

        @Override
        public BookieSocketAddress replaceBookie(int ensembleSize, int writeQuorumSize,
            int ackQuorumSize, Map<String, byte[]> customMetadata, Set<BookieSocketAddress> currentEnsemble,
            BookieSocketAddress bookieToReplace, Set<BookieSocketAddress> excludeBookies)
            throws BKException.BKNotEnoughBookiesException {
            new Exception("replaceBookie " + ensembleSize + "," + customMetadata).printStackTrace();
            assertNotNull(customMetadata);
            customMetadataOnReplaceBookieStack.add(customMetadata);
            return super.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata,
                currentEnsemble, bookieToReplace, excludeBookies);
        }

        @Override
        public ArrayList<BookieSocketAddress> newEnsemble(int ensembleSize, int quorumSize,
            int ackQuorumSize, Map<String, byte[]> customMetadata, Set<BookieSocketAddress> excludeBookies)
            throws BKException.BKNotEnoughBookiesException {
            assertNotNull(customMetadata);
            customMetadataOnNewEnsembleStack.add(customMetadata);
            return super.newEnsemble(ensembleSize, quorumSize, ackQuorumSize, customMetadata, excludeBookies);
        }
    }

    @Before
    public void reset() {
        customMetadataOnNewEnsembleStack.clear();
        customMetadataOnReplaceBookieStack.clear();
    }

    @Test
    public void testNewEnsemble() throws Exception {
        numBookies = 1;
        startBKCluster(zkUtil.getMetadataServiceUri());
        try {
            Map<String, byte[]> customMetadata = new HashMap<>();
            customMetadata.put(property, value);
            try (BookKeeper bk = new BookKeeper(baseClientConf, zkc)) {
                bk.createLedger(1, 1, 1, digestType, PASSWORD.getBytes(), customMetadata);
            }
            assertEquals(1, customMetadataOnNewEnsembleStack.size());
            assertArrayEquals(value, customMetadataOnNewEnsembleStack.get(0).get(property));
        } finally {
            stopBKCluster();
        }
    }

    @Test
    public void testNewEnsembleWithNotEnoughtBookies() throws Exception {
        numBookies = 0;
        try {
            startBKCluster(zkUtil.getMetadataServiceUri());
            Map<String, byte[]> customMetadata = new HashMap<>();
            customMetadata.put(property, value);
            try (BookKeeper bk = new BookKeeper(baseClientConf, zkc)) {
                bk.createLedger(1, 1, 1, digestType, PASSWORD.getBytes(), customMetadata);
                fail("creation should fail");
            } catch (BKException.BKNotEnoughBookiesException bneb) {
            }
            assertEquals(2, customMetadataOnNewEnsembleStack.size());
            assertArrayEquals(value, customMetadataOnNewEnsembleStack.get(0).get(property));
            assertArrayEquals(value, customMetadataOnNewEnsembleStack.get(1).get(property));
        } finally {
            stopBKCluster();
        }
    }

    @Test
    public void testReplaceBookie() throws Exception {
        numBookies = 3;
        startBKCluster(zkUtil.getMetadataServiceUri());
        try {
            Map<String, byte[]> customMetadata = new HashMap<>();
            customMetadata.put(property, value);
            try (BookKeeper bk = new BookKeeper(baseClientConf, zkc)) {
                try (LedgerHandle lh = bk.createLedger(2, 2, 2, digestType, PASSWORD.getBytes(), customMetadata)) {
                    lh.addEntry(value);
                    long lId = lh.getId();
                    List<BookieSocketAddress> ensembleAtFirstEntry = lh.getLedgerMetadata().getEnsembleAt(lId);
                    assertEquals(2, ensembleAtFirstEntry.size());
                    killBookie(ensembleAtFirstEntry.get(0));
                    lh.addEntry(value);
                }
            }
            assertEquals(2, customMetadataOnNewEnsembleStack.size());
            assertArrayEquals(value, customMetadataOnNewEnsembleStack.get(0).get(property));
            // replaceBookie by default calls newEnsemble, so newEnsemble gets called twice
            assertArrayEquals(value, customMetadataOnNewEnsembleStack.get(0).get(property));

            assertEquals(1, customMetadataOnReplaceBookieStack.size());
            assertArrayEquals(value, customMetadataOnReplaceBookieStack.get(0).get(property));

        } finally {
            stopBKCluster();
        }
    }

}
