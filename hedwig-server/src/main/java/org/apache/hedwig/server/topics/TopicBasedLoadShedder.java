/**
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
package org.apache.hedwig.server.topics;

import java.util.List;
import java.util.Map;

import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * Shed load by releasing topics.
 */
public class TopicBasedLoadShedder {
    private static final Logger logger = LoggerFactory.getLogger(TopicBasedLoadShedder.class);
    private final double tolerancePercentage;
    private final long maxLoadToShed;
    private final TopicManager tm;
    private final List<ByteString> topicList;

    /**
     * @param tm The topic manager used to handle load shedding
     * @param tolerancePercentage The tolerance percentage for shedding load
     * @param maxLoadToShed The maximum amoung of load to shed in one call.
     */
    public TopicBasedLoadShedder(TopicManager tm, double tolerancePercentage,
                                 HubLoad maxLoadToShed) {
        // Make sure that all functions in this class have a consistent view
        // of the load. So, we use the same topic list throughout.
        this(tm, tm.getTopicList(), tolerancePercentage, maxLoadToShed);
    }

    /**
     * This is public because it makes testing easier.
     * @param tm The topic manager used to handle load shedding
     * @param topicList The topic list representing topics owned by this hub.
     * @param tolerancePercentage The tolerance percentage for shedding load
     * @param maxLoadToShed The maximum amoung of load to shed in one call.
     */
    TopicBasedLoadShedder(TopicManager tm, List<ByteString> topicList,
                          double tolerancePercentage,
                          HubLoad maxLoadToShed) {
        this.tolerancePercentage = tolerancePercentage;
        this.maxLoadToShed = maxLoadToShed.getNumTopics();
        this.tm = tm;
        this.topicList = topicList;
    }

    /**
     * Reduce the load on the current hub so that it reaches the target load.
     * We reduce load by releasing topics using the {@link TopicManager} passed
     * to the constructor. We use {@link TopicManager#releaseTopics(int, org.apache.hedwig.util.Callback, Object)}
     * to actually release topics.
     *
     * @param targetLoad
     * @param callback
     *              a Callback<Long> that indicates how many topics we tried to release.
     * @param ctx
     */
    public void reduceLoadTo(HubLoad targetLoad, final Callback<Long> callback, final Object ctx) {
        int targetTopics = (int)targetLoad.toHubLoadData().getNumTopics();
        int numTopicsToRelease = topicList.size() - targetTopics;

        // The number of topics we own is less than the target topic size. We don't release
        // any topics in this case.
        if (numTopicsToRelease <= 0) {
            callback.operationFinished(ctx, 0L);
            return;
        }
        // Call releaseTopics() on the topic manager to do this. We let the manager handle the release
        // policy.
        tm.releaseTopics(numTopicsToRelease, callback, ctx);
    }

    /**
     * Calculate the average number of topics on the currently active hubs and release topics
     * if required.
     * We shed topics if we currently hold topics greater than average + average * tolerancePercentage/100.0
     * We shed a maximum of maxLoadToShed topics
     * We also hold on to at least one topic.
     * @param loadMap
     * @param callback
     *          A return value of true means we tried to rebalance. False means that there was
     *          no need to rebalance.
     * @param ctx
     */
    public void shedLoad(final Map<HubInfo, HubLoad> loadMap, final Callback<Boolean> callback,
                         final Object ctx) {

        long totalTopics = 0L;
        long myTopics = topicList.size();
        for (Map.Entry<HubInfo, HubLoad> entry : loadMap.entrySet()) {
            if (null == entry.getKey() || null == entry.getValue()) {
                continue;
            }
            totalTopics += entry.getValue().toHubLoadData().getNumTopics();
        }

        double averageTopics = (double)totalTopics/loadMap.size();
        logger.info("Total topics in the cluster : {}. Average : {}.", totalTopics, averageTopics);

        // Handle the case when averageTopics == 0. We hold on to at least 1 topic.
        long permissibleTopics =
            Math.max(1L, (long) Math.ceil(averageTopics + averageTopics * tolerancePercentage / 100.0));
        logger.info("Permissible topics : {}. Number of topics this hub holds : {}.", permissibleTopics, myTopics);
        if (myTopics <= permissibleTopics) {
            // My owned topics are less than those permitted by the current tolerance level. No need to release
            // any topics.
            callback.operationFinished(ctx, false);
            return;
        }

        // The number of topics I own is more than what I should be holding. We shall now attempt to shed some load.
        // We shed at most maxLoadToShed number of topics. We also hold on to at least 1 topic.
        long targetNumTopics = Math.max(1L, Math.max((long)Math.ceil(averageTopics), myTopics - maxLoadToShed));

        // Reduce the load on the current hub to the target load we calculated above.
        logger.info("Reducing load on this hub to {} topics.", targetNumTopics);
        reduceLoadTo(new HubLoad(targetNumTopics), new Callback<Long>() {
            @Override
            public void operationFinished(Object ctx, Long numReleased) {
                logger.info("Released {} topics to shed load.", numReleased);
                callback.operationFinished(ctx, true);
            }

            @Override
            public void operationFailed(Object ctx, PubSubException e) {
                callback.operationFailed(ctx, e);
            }
        }, ctx);
    }
}
