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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WeightedRandomSelectionImpl<T> implements WeightedRandomSelection<T> {
    static final Logger LOG = LoggerFactory.getLogger(WeightedRandomSelectionImpl.class);

    Double randomMax;
    int maxProbabilityMultiplier;
    Map<T, WeightedObject> map;
    TreeMap<Double, T> cummulativeMap = new TreeMap<Double, T>();
    ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    WeightedRandomSelectionImpl() {
        maxProbabilityMultiplier = -1;
    }

    WeightedRandomSelectionImpl(int maxMultiplier) {
        this.maxProbabilityMultiplier = maxMultiplier;
    }

    @Override
    public void updateMap(Map<T, WeightedObject> map) {
        // get the sum total of all the values; this will be used to
        // calculate the weighted probability later on
        Long totalWeight = 0L, min = Long.MAX_VALUE;
        List<WeightedObject> values = new ArrayList<WeightedObject>(map.values());
        Collections.sort(values, new Comparator<WeightedObject>() {
            @Override
            public int compare(WeightedObject o1, WeightedObject o2) {
                long diff = o1.getWeight() - o2.getWeight();
                if (diff < 0L) {
                    return -1;
                } else if (diff > 0L) {
                    return 1;
                } else {
                    return 0;
                }
            }
        });
        for (int i = 0; i < values.size(); i++) {
            totalWeight += values.get(i).getWeight();
            if (values.get(i).getWeight() != 0 && min > values.get(i).getWeight()) {
                min = values.get(i).getWeight();
            }
        }

        double median = 0;
        if (totalWeight == 0) {
            // all the values are zeros; assign a value of 1 to all and the totalWeight equal
            // to the size of the values
            min = 1L;
            median = 1;
            totalWeight = (long) values.size();
        } else {
            int mid = values.size() / 2;
            if ((values.size() % 2) == 1) {
                median = values.get(mid).getWeight();
            } else {
                median = (double) (values.get(mid - 1).getWeight() + values.get(mid).getWeight()) / 2;
            }
        }

        double medianWeight, minWeight;
        medianWeight = median / (double) totalWeight;
        minWeight = (double) min / totalWeight;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating weights map. MediaWeight: {} MinWeight: {}", medianWeight, minWeight);
        }

        double maxWeight = maxProbabilityMultiplier * medianWeight;
        Map<T, Double> weightMap = new HashMap<T, Double>();
        for (Map.Entry<T, WeightedObject> e : map.entrySet()) {
            double weightedProbability;
            if (e.getValue().getWeight() > 0) {
                weightedProbability = (double) e.getValue().getWeight() / (double) totalWeight;
            } else {
                weightedProbability = minWeight;
            }
            if (maxWeight > 0 && weightedProbability > maxWeight) {
                weightedProbability = maxWeight;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Capping the probability to {} for {} Value: {}",
                            weightedProbability, e.getKey(), e.getValue());
                }
            }
            weightMap.put(e.getKey(), weightedProbability);
        }

        // The probability of picking a bookie randomly is defaultPickProbability
        // but we change that priority by looking at the weight that each bookie
        // carries.
        TreeMap<Double, T> tmpCummulativeMap = new TreeMap<Double, T>();
        Double key = 0.0;
        for (Map.Entry<T, Double> e : weightMap.entrySet()) {
            tmpCummulativeMap.put(key, e.getKey());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Key: {} Value: {} AssignedKey: {} AssignedWeight: {}",
                        e.getKey(), e.getValue(), key, e.getValue());
            }
            key += e.getValue();
        }

        rwLock.writeLock().lock();
        try {
            this.map = map;
            cummulativeMap = tmpCummulativeMap;
            randomMax = key;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public T getNextRandom() {
        rwLock.readLock().lock();
        try {
            // pick a random number between 0 and randMax
            Double randomNum = randomMax * Math.random();
            // find the nearest key in the map corresponding to the randomNum
            Double key = cummulativeMap.floorKey(randomNum);
            //LOG.info("Random max: {} CummulativeMap size: {} selected key: {}", randomMax, cummulativeMap.size(),
            //    key);
            return cummulativeMap.get(key);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void setMaxProbabilityMultiplier(int max) {
        this.maxProbabilityMultiplier = max;
    }

    @Override
    public T getNextRandom(Collection<T> selectedNodes) {
        throw new UnsupportedOperationException("getNextRandom is not implemented for WeightedRandomSelectionImpl");
    }

}
