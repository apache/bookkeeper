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

import com.google.common.math.Quantiles;
import com.google.common.math.Quantiles.ScaleAndIndex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DynamicWeightedRandomSelectionImpl class implements both getNextRandom
 * overloaded methods. Where getNextRandom() method considers all bookies it
 * knows of as candidates, but getNextRandom(Collection selectedNodes) method
 * considers only 'selectedNodes' as candidates.
 */
class DynamicWeightedRandomSelectionImpl<T> implements WeightedRandomSelection<T> {
    static final Logger LOG = LoggerFactory.getLogger(DynamicWeightedRandomSelectionImpl.class);

    int maxProbabilityMultiplier;
    final Map<T, WeightedObject> weightMap;
    final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);
    Random rand;

    DynamicWeightedRandomSelectionImpl() {
        this(-1);
    }

    DynamicWeightedRandomSelectionImpl(int maxMultiplier) {
        this.maxProbabilityMultiplier = maxMultiplier;
        this.weightMap = new HashMap<T, WeightedObject>();
        rand = new Random(System.currentTimeMillis());
    }

    @Override
    public void updateMap(Map<T, WeightedObject> updatedMap) {
        rwLock.writeLock().lock();
        try {
            weightMap.clear();
            weightMap.putAll(updatedMap);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public T getNextRandom() {
        rwLock.readLock().lock();
        try {
            return getNextRandom(weightMap.keySet());
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public T getNextRandom(Collection<T> selectedNodes) {
        rwLock.readLock().lock();
        try {
            /*
             * calculate minWeight and actual total weight.
             */
            long minWeight = Long.MAX_VALUE;
            long actTotalWeight = 0;
            for (T node : selectedNodes) {
                long weight = 0;
                if ((weightMap.containsKey(node))) {
                    weight = weightMap.get(node).getWeight();
                }
                actTotalWeight += weight;
                if (weight > 0 && minWeight > weight) {
                    minWeight = weight;
                }
            }

            long medianWeight;
            /*
             * if actTotalWeight is 0, then assign 1 to minWeight and
             * medianWeight.
             */
            if (actTotalWeight == 0) {
                minWeight = 1L;
                medianWeight = 1L;
            } else {
                /*
                 * calculate medianWeight.
                 */
                Function<? super T, ? extends Long> weightFunc = (node) -> {
                    long weight = 0;
                    if ((weightMap.containsKey(node))) {
                        weight = weightMap.get(node).getWeight();
                    }
                    return Long.valueOf(weight);
                };
                ArrayList<Long> weightList = selectedNodes.stream().map(weightFunc)
                        .collect(Collectors.toCollection(ArrayList::new));
                ScaleAndIndex median = Quantiles.median();
                medianWeight = (long) median.compute(weightList);
            }

            /*
             * initialize maxWeight value based on maxProbabilityMultiplier.
             */
            long maxWeight = maxProbabilityMultiplier * medianWeight;

            /*
             * apply weighted random selection to select an element randomly
             * based on weight.
             */
            long cumTotalWeight = 0;
            T nextRandomNode = null;
            for (T node : selectedNodes) {
                long weight = 0;
                if ((weightMap.containsKey(node))) {
                    weight = weightMap.get(node).getWeight();
                }
                if (weight <= 0) {
                    weight = minWeight;
                } else if (maxWeight > 0 && weight > maxWeight) {
                    weight = maxWeight;
                }
                long tmpRandLong = rand.nextLong();
                if (tmpRandLong == Long.MIN_VALUE) {
                    tmpRandLong++;
                }
                long randValue = Math.abs(tmpRandLong) % (cumTotalWeight + weight);
                if (randValue >= cumTotalWeight) {
                    nextRandomNode = node;
                }
                cumTotalWeight += weight;
            }
            return nextRandomNode;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void setMaxProbabilityMultiplier(int max) {
        this.maxProbabilityMultiplier = max;
    }
}
