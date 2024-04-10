package org.apache.bookkeeper.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LoadWeightedSelectionImpl<T> implements WeightedRandomSelection<T> {
    static final Logger LOG = LoggerFactory.getLogger(LoadWeightedSelectionImpl.class);

    Double randomMax;
    long minWeight = 10;
    long maxWeight = 100;
    long overLoadWeight = 5;
    Map<T, WeightedObject> map;
    TreeMap<Double, T> cummulativeMap = new TreeMap<Double, T>();
    ReadWriteLock rwLock = new ReentrantReadWriteLock(true);
    int loadThreshold;
    double lowLoadBookieRatio;

    LoadWeightedSelectionImpl(int loadThreshold, double lowLoadBookieRatio) {
        this.loadThreshold = loadThreshold;
        this.lowLoadBookieRatio = lowLoadBookieRatio;
    }

    @Override
    public void updateMap(Map<T, WeightedObject> map) {
        // step 1: exclude the high load bookie, whose load is higher than threshold, default 70%
        Map<T, WeightedObject> lowLoadRackMap = new HashMap<>();
        for (Map.Entry<T, WeightedObject> e : map.entrySet()) {
            if (e.getValue().getLoad() < 0) {
                // if getLoad() is -1, it means the bookie maybe disable baseMetricMonitorEnabled
                // or broker can not get load information from bookie because of restApi timeout or exception.
                // current method: we only skip these -1 bookie
                // Alternative method: we can give a default load for these -1 bookie
            } else if (e.getValue().getLoad() <= loadThreshold) {
                lowLoadRackMap.put(e.getKey(), e.getValue());
            } else {
                // just exclude the high load bookie
            }
        }

        // The probability of picking a bookie randomly is defaultPickProbability
        // but we may change that priority by looking at the weight that each bookie
        // carries.
        TreeMap<Double, T> tmpCummulativeMap = new TreeMap<>();
        Double key = 0.0;

        if (lowLoadRackMap.size() <= map.size() * lowLoadBookieRatio) {
            // corner case: if most bookie not have bookieInfo, or load is more than threshold,
            // the bookie's number which can be selected would be a little.
            // And this would cause ledger only can select from a little bookies,
            // maybe cause write throughput incline problem.
            // -----
            // so if low-load-bookie's number <= all-selected-bookie's number * ratio,
            // fallback to random select of all bookies
            Double defaultPickProbability = 1d / map.size();
            for (Map.Entry<T, WeightedObject> e : map.entrySet()) {
                tmpCummulativeMap.put(key, e.getKey());
                key += defaultPickProbability;
            }
        } else {
            // step2: roulette wheel selection
            // default bookies's weight is writeBytePerSecond, aiming to balance bookie throughput
            map = lowLoadRackMap;

            // get the sum total of all the values; this will be used to
            // calculate the weighted probability later on
            Long totalWeight = 0L;
            List<WeightedObject> values = new ArrayList<>(map.values());
            for (int i = 0; i < values.size(); i++) {
                // case1: if getLoad() == -1, the bookie should be excluded.
                // but if getLoad() != -1, getWeight() == -1, it still have problem.
                // so deal with this case, although it should not happen.
                if (values.get(i).getWeight() < 0) {
                    continue;
                }
                // bookie's weight should be 0-100
                // case 2: if getWeight() > 100, this bookie is overload and
                // should be regard as overLoadWeight, default is 5
                if (values.get(i).getWeight() > maxWeight) {
                    totalWeight += overLoadWeight;
                    continue;
                }
                // case3: (100 - bookie's weight) < 10, smooth the probability.
                // because 0-10's probability differ so much from 10-100's probability.
                // we don't want this big difference cause write incline problem.
                totalWeight += Math.max((maxWeight - values.get(i).getWeight()), minWeight);
            }

            Map<T, Double> weightMap = new HashMap<>();
            for (Map.Entry<T, WeightedObject> e : map.entrySet()) {
                double weightedProbability;
                if (e.getValue().getWeight() < 0) {
                    LOG.error("should not occur this case in LoadWeightedSelectionImpl." +
                                    " getLoad() != -1, but getWeight() is {}. bookie is {}",
                            e.getValue().getWeight(), e.getKey());
                    continue;
                }
                if (e.getValue().getWeight() > maxWeight) {
                    weightedProbability = (double) overLoadWeight / (double) totalWeight;
                    weightMap.put(e.getKey(), weightedProbability);
                    continue;
                }
                // the higher load weight, the less probability
                weightedProbability = (double) Math.max((maxWeight - e.getValue().getWeight()), minWeight) /
                        (double) totalWeight;
                weightMap.put(e.getKey(), weightedProbability);
            }


            for (Map.Entry<T, Double> e : weightMap.entrySet()) {
                tmpCummulativeMap.put(key, e.getKey());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Key: {} Value: {} AssignedKey: {} AssignedWeight: {}",
                            e.getKey(), e.getValue(), key, e.getValue());
                }
                key += e.getValue();
            }
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
            return cummulativeMap.get(key);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void setMaxProbabilityMultiplier(int max) {

    }

    @Override
    public int getSize() {
        rwLock.readLock().lock();
        try {
            return cummulativeMap.size();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public T getNextRandom(Collection<T> selectedNodes) {
        throw new UnsupportedOperationException("getNextRandom is not implemented for WeightedRandomSelectionImpl");
    }

}
