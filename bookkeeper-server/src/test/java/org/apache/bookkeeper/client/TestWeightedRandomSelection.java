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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.bookkeeper.client.WeightedRandomSelection.WeightedObject;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test weighted random selection methods.
 */
@RunWith(Parameterized.class)
public class TestWeightedRandomSelection {

    static final Logger LOG = LoggerFactory.getLogger(TestWeightedRandomSelection.class);

    static class TestObj implements WeightedObject {
        long val;

        TestObj(long value) {
            this.val = value;
        }

        @Override
        public long getWeight() {
            return val;
        }
    }

    Class<? extends WeightedRandomSelection> weightedRandomSelectionClass;
    WeightedRandomSelection<String> wRS;
    Configuration conf = new CompositeConfiguration();
    int multiplier = 3;

    @Parameters
    public static Collection<Object[]> weightedRandomSelectionClass() {
        return Arrays.asList(
                new Object[][] { { WeightedRandomSelectionImpl.class }, { DynamicWeightedRandomSelectionImpl.class } });
    }

    public TestWeightedRandomSelection(Class<? extends WeightedRandomSelection> weightedRandomSelectionClass) {
        this.weightedRandomSelectionClass = weightedRandomSelectionClass;
    }

    @Before
    public void setUp() throws Exception {
        if (weightedRandomSelectionClass.equals(WeightedRandomSelectionImpl.class)) {
            wRS = new WeightedRandomSelectionImpl<String>();
        } else {
            wRS = new DynamicWeightedRandomSelectionImpl<String>();
        }
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testSelectionWithEqualWeights() throws Exception {
        Map<String, WeightedObject> map = new HashMap<String, WeightedObject>();

        Long val = 100L;
        int numKeys = 50, totalTries = 1000000;
        Map<String, Integer> randomSelection = new HashMap<String, Integer>();
        for (Integer i = 0; i < numKeys; i++) {
            map.put(i.toString(), new TestObj(val));
            randomSelection.put(i.toString(), 0);
        }

        wRS.updateMap(map);
        for (int i = 0; i < totalTries; i++) {
            String key = wRS.getNextRandom();
            randomSelection.put(key, randomSelection.get(key) + 1);
        }

        // there should be uniform distribution
        double expectedPct = ((double) 1 / (double) numKeys) * 100;
        for (Map.Entry<String, Integer> e : randomSelection.entrySet()) {
            double actualPct = ((double) e.getValue() / (double) totalTries) * 100;
            double delta = (Math.abs(expectedPct - actualPct) / expectedPct) * 100;
            System.out.println("Key:" + e.getKey() + " Value:" + e.getValue() + " Expected: " + expectedPct
                    + " Actual: " + actualPct + " delta: " + delta);
            // should be within 5% of expected
            assertTrue("Not doing uniform selection when weights are equal", delta < 5);
        }
    }

    @Test
    public void testSelectionWithAllZeroWeights() throws Exception {
        Map<String, WeightedObject> map = new HashMap<String, WeightedObject>();

        int numKeys = 50, totalTries = 1000000;
        Map<String, Integer> randomSelection = new HashMap<String, Integer>();
        for (Integer i = 0; i < numKeys; i++) {
            map.put(i.toString(), new TestObj(0L));
            randomSelection.put(i.toString(), 0);
        }

        wRS.updateMap(map);
        for (int i = 0; i < totalTries; i++) {
            String key = wRS.getNextRandom();
            randomSelection.put(key, randomSelection.get(key) + 1);
        }

        // when all the values are zeros, there should be uniform distribution
        double expectedPct = ((double) 1 / (double) numKeys) * 100;
        for (Map.Entry<String, Integer> e : randomSelection.entrySet()) {
            double actualPct = ((double) e.getValue() / (double) totalTries) * 100;
            double delta = (Math.abs(expectedPct - actualPct) / expectedPct) * 100;
            System.out.println("Key:" + e.getKey() + " Value:" + e.getValue() + " Expected: " + expectedPct
                    + " Actual: " + actualPct);
            // should be within 5% of expected
            assertTrue("Not doing uniform selection when weights are equal", delta < 5);
        }
    }

    void verifyResult(Map<String, WeightedObject> map, Map<String, Integer> randomSelection, int multiplier,
            long minWeight, long medianWeight, long totalWeight, int totalTries) {
        List<Integer> values = new ArrayList<Integer>(randomSelection.values());
        Collections.sort(values);
        double medianObserved, medianObservedWeight, medianExpectedWeight;
        int mid = values.size() / 2;
        if ((values.size() % 2) == 1) {
            medianObserved = values.get(mid);
        } else {
            medianObserved = (double) (values.get(mid - 1) + values.get(mid)) / 2;
        }

        medianObservedWeight = (double) medianObserved / (double) totalTries;
        medianExpectedWeight = (double) medianWeight / totalWeight;

        for (Map.Entry<String, Integer> e : randomSelection.entrySet()) {
            double observed = (((double) e.getValue() / (double) totalTries));

            double expected;
            if (map.get(e.getKey()).getWeight() == 0) {
                // if the value is 0 for any key, we make it equal to the first
                // non zero value
                expected = (double) minWeight / (double) totalWeight;
            } else {
                expected = (double) map.get(e.getKey()).getWeight() / (double) totalWeight;
            }
            if (multiplier > 0 && expected > multiplier * medianExpectedWeight) {
                expected = multiplier * medianExpectedWeight;
            }
            // We can't compare these weights because they are derived from
            // different
            // values. But if we express them as a multiple of the min in each,
            // then
            // they should be comparable
            double expectedMultiple = expected / medianExpectedWeight;
            double observedMultiple = observed / medianObservedWeight;
            double delta = (Math.abs(expectedMultiple - observedMultiple) / expectedMultiple) * 100;
            System.out.println("Key:" + e.getKey() + " Value:" + e.getValue() + " Expected " + expectedMultiple
                    + " actual " + observedMultiple + " delta " + delta + "%");

            // the observed should be within 5% of expected
            assertTrue("Not doing uniform selection when weights are equal", delta < 5);
        }
    }

    @Test
    public void testSelectionWithSomeZeroWeights() throws Exception {
        Map<String, WeightedObject> map = new HashMap<String, WeightedObject>();
        Map<String, Integer> randomSelection = new HashMap<String, Integer>();
        int numKeys = 50;
        multiplier = 3;
        long val = 0L, total = 0L, minWeight = 100L, medianWeight = minWeight;
        wRS.setMaxProbabilityMultiplier(multiplier);
        for (Integer i = 0; i < numKeys; i++) {
            if (i < numKeys / 3) {
                val = 0L;
            } else if (i < 2 * (numKeys / 3)) {
                val = minWeight;
            } else {
                val = 2 * minWeight;
            }
            total += val;
            map.put(i.toString(), new TestObj(val));
            randomSelection.put(i.toString(), 0);
        }

        wRS.updateMap(map);
        int totalTries = 1000000;
        for (int i = 0; i < totalTries; i++) {
            String key = wRS.getNextRandom();
            randomSelection.put(key, randomSelection.get(key) + 1);
        }
        verifyResult(map, randomSelection, multiplier, minWeight, medianWeight, total, totalTries);
    }

    @Test
    public void testSelectionWithUnequalWeights() throws Exception {
        Map<String, WeightedObject> map = new HashMap<String, WeightedObject>();
        Map<String, Integer> randomSelection = new HashMap<String, Integer>();
        int numKeys = 50;
        multiplier = 4;
        long val = 0L, total = 0L, minWeight = 100L, medianWeight = 2 * minWeight;
        wRS.setMaxProbabilityMultiplier(multiplier);
        for (Integer i = 0; i < numKeys; i++) {
            if (i < numKeys / 3) {
                val = minWeight;
            } else if (i < 2 * (numKeys / 3)) {
                val = 2 * minWeight;
            } else {
                val = 10 * minWeight;
            }
            total += val;
            map.put(i.toString(), new TestObj(val));
            randomSelection.put(i.toString(), 0);
        }

        wRS.updateMap(map);
        int totalTries = 1000000;
        for (int i = 0; i < totalTries; i++) {
            String key = wRS.getNextRandom();
            randomSelection.put(key, randomSelection.get(key) + 1);
        }
        verifyResult(map, randomSelection, multiplier, minWeight, medianWeight, total, totalTries);
    }

    @Test
    public void testSelectionWithHotNode() throws Exception {
        Map<String, WeightedObject> map = new HashMap<String, WeightedObject>();
        Map<String, Integer> randomSelection = new HashMap<String, Integer>();

        multiplier = 3; // no max
        int numKeys = 50;
        long total = 0L, minWeight = 100L, val = minWeight, medianWeight = minWeight;
        wRS.setMaxProbabilityMultiplier(multiplier);
        for (Integer i = 0; i < numKeys; i++) {
            if (i == numKeys - 1) {
                // last one has 10X more weight than the rest put together
                val = 10 * (numKeys - 1) * 100L;
            }
            total += val;
            map.put(i.toString(), new TestObj(val));
            randomSelection.put(i.toString(), 0);
        }

        wRS.updateMap(map);
        int totalTries = 1000000;
        for (int i = 0; i < totalTries; i++) {
            String key = wRS.getNextRandom();
            randomSelection.put(key, randomSelection.get(key) + 1);
        }
        verifyResult(map, randomSelection, multiplier, minWeight, medianWeight, total, totalTries);
    }

    @Test
    public void testSelectionWithHotNodeWithLimit() throws Exception {
        Map<String, WeightedObject> map = new HashMap<String, WeightedObject>();
        Map<String, Integer> randomSelection = new HashMap<String, Integer>();

        multiplier = 3; // limit the max load on hot node to be 3X
        int numKeys = 50;
        long total = 0L, minWeight = 100L, val = minWeight, medianWeight = minWeight;
        wRS.setMaxProbabilityMultiplier(multiplier);
        for (Integer i = 0; i < numKeys; i++) {
            if (i == numKeys - 1) {
                // last one has 10X more weight than the rest put together
                val = 10 * (numKeys - 1) * 100L;
            }
            total += val;
            map.put(i.toString(), new TestObj(val));
            randomSelection.put(i.toString(), 0);
        }

        wRS.updateMap(map);
        int totalTries = 1000000;
        for (int i = 0; i < totalTries; i++) {
            String key = wRS.getNextRandom();
            randomSelection.put(key, randomSelection.get(key) + 1);
        }
        verifyResult(map, randomSelection, multiplier, minWeight, medianWeight, total, totalTries);
    }

    @Test
    public void testSelectionFromSelectedNodesWithEqualWeights() throws Exception {
        /*
         * this testcase is for only DynamicWeightedRandomSelectionImpl
         */
        Assume.assumeTrue(weightedRandomSelectionClass.equals(DynamicWeightedRandomSelectionImpl.class));
        Map<String, WeightedObject> map = new HashMap<String, WeightedObject>();

        Long val = 100L;
        int numKeys = 50, totalTries = 1000;
        Map<String, Integer> randomSelection = new HashMap<String, Integer>();
        for (Integer i = 0; i < numKeys; i++) {
            map.put(i.toString(), new TestObj(val));
            randomSelection.put(i.toString(), 0);
        }

        Set<String> selectFrom = new HashSet<String>();
        for (int i = 0; i < numKeys / 2; i++) {
            selectFrom.add(Integer.toString(i));
        }

        wRS.updateMap(map);
        for (int i = 0; i < totalTries; i++) {
            String selectedKey = wRS.getNextRandom(selectFrom);
            assertTrue("NextRandom key should be from selected list", selectFrom.contains(selectedKey));
        }
    }
}
