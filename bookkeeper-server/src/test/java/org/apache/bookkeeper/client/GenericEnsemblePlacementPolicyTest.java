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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.resolvers.BeforeParameterResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Testing a generic ensemble placement policy.
 */
@ExtendWith(BeforeParameterResolver.class)
public class GenericEnsemblePlacementPolicyTest extends BookKeeperClusterTestCase {

    private BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
    private static final String PASSWORD = "testPasswd";
    private static final String property = "foo";
    private static final byte[] value = "bar".getBytes(StandardCharsets.UTF_8);
    private static List<Map<String, byte[]>> customMetadataOnNewEnsembleStack = new ArrayList<>();
    private static List<Map<String, byte[]>> customMetadataOnReplaceBookieStack = new ArrayList<>();

    @BeforeEach
    public void init(boolean diskWeightBasedPlacementEnabled) {
        baseClientConf.setDiskWeightBasedPlacementEnabled(diskWeightBasedPlacementEnabled);
    }

    public static Collection<Object[]> getDiskWeightBasedPlacementEnabled() {
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    public GenericEnsemblePlacementPolicyTest() {
        super(0);
        baseClientConf.setEnsemblePlacementPolicy(CustomEnsemblePlacementPolicy.class);
    }

    /**
     * A custom ensemble placement policy.
     */
    public static final class CustomEnsemblePlacementPolicy extends DefaultEnsemblePlacementPolicy {

        @Override
        public PlacementResult<BookieId> replaceBookie(int ensembleSize, int writeQuorumSize,
            int ackQuorumSize, Map<String, byte[]> customMetadata, List<BookieId> currentEnsemble,
            BookieId bookieToReplace, Set<BookieId> excludeBookies)
            throws BKException.BKNotEnoughBookiesException {
            new Exception("replaceBookie " + ensembleSize + "," + customMetadata).printStackTrace();
            assertNotNull(customMetadata);
            customMetadataOnReplaceBookieStack.add(customMetadata);
            return super.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata,
                currentEnsemble, bookieToReplace, excludeBookies);
        }

        @Override
        public PlacementResult<List<BookieId>> newEnsemble(int ensembleSize, int quorumSize,
            int ackQuorumSize, Map<String, byte[]> customMetadata, Set<BookieId> excludeBookies)
            throws BKException.BKNotEnoughBookiesException {
            assertNotNull(customMetadata);
            customMetadataOnNewEnsembleStack.add(customMetadata);
            return super.newEnsemble(ensembleSize, quorumSize, ackQuorumSize, customMetadata, excludeBookies);
        }
    }

    @BeforeEach
    void reset() {
        customMetadataOnNewEnsembleStack.clear();
        customMetadataOnReplaceBookieStack.clear();
    }

    @MethodSource("getDiskWeightBasedPlacementEnabled")
    @ParameterizedTest
    public void newEnsemble(boolean diskWeightBasedPlacementEnabled) throws Exception {
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

    @MethodSource("getDiskWeightBasedPlacementEnabled")
    @ParameterizedTest
    public void newEnsembleWithNotEnoughBookies(boolean diskWeightBasedPlacementEnabled)
        throws Exception {
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

    @MethodSource("getDiskWeightBasedPlacementEnabled")
    @ParameterizedTest
    public void replaceBookie(boolean diskWeightBasedPlacementEnabled) throws Exception {
        numBookies = 3;
        startBKCluster(zkUtil.getMetadataServiceUri());
        try {
            Map<String, byte[]> customMetadata = new HashMap<>();
            customMetadata.put(property, value);
            try (BookKeeper bk = new BookKeeper(baseClientConf, zkc)) {
                try (LedgerHandle lh = bk.createLedger(2, 2, 2, digestType, PASSWORD.getBytes(), customMetadata)) {
                    lh.addEntry(value);
                    long lId = lh.getId();
                    List<BookieId> ensembleAtFirstEntry = lh.getLedgerMetadata().getEnsembleAt(lId);
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
