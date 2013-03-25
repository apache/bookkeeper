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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.common.TopicOpQueuer;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.CallbackUtils;
import org.apache.hedwig.util.HedwigSocketAddress;

public abstract class AbstractTopicManager implements TopicManager {

    /**
     * Stats for a topic. For now it just an empty stub class.
     */
    static class TopicStats {
    }

    final static TopicStats STUB_TOPIC_STATS = new TopicStats();

    /**
     * My name.
     */
    protected HedwigSocketAddress addr;

    /**
     * Topic change listeners.
     */
    protected ArrayList<TopicOwnershipChangeListener> listeners = new ArrayList<TopicOwnershipChangeListener>();

    /**
     * List of topics I believe I am responsible for.
     */
    protected Cache<ByteString, TopicStats> topics;

    protected TopicOpQueuer queuer;
    protected ServerConfiguration cfg;
    protected ScheduledExecutorService scheduler;

    private static final Logger logger = LoggerFactory.getLogger(AbstractTopicManager.class);

    private class GetOwnerOp extends TopicOpQueuer.AsynchronousOp<HedwigSocketAddress> {
        public boolean shouldClaim;

        public GetOwnerOp(final ByteString topic, boolean shouldClaim,
                          final Callback<HedwigSocketAddress> cb, Object ctx) {
            queuer.super(topic, cb, ctx);
            this.shouldClaim = shouldClaim;
        }

        @Override
        public void run() {
            realGetOwner(topic, shouldClaim, cb, ctx);
        }
    }

    private class ReleaseOp extends TopicOpQueuer.AsynchronousOp<Void> {
        final boolean checkExistence;

        public ReleaseOp(ByteString topic, Callback<Void> cb, Object ctx) {
            this(topic, cb, ctx, true);
        }

        ReleaseOp(ByteString topic, Callback<Void> cb, Object ctx,
                  boolean checkExistence) {
            queuer.super(topic, cb, ctx);
            this.checkExistence = checkExistence;
        }

        @Override
        public void run() {
            if (checkExistence) {
                TopicStats stats = topics.getIfPresent(topic);
                if (null == stats) {
                    cb.operationFinished(ctx, null);
                    return;
                }
            }
            realReleaseTopic(topic, cb, ctx);
        }
    }

    /**
     * Release topic when the topic is removed from topics cache.
     */
    class ReleaseTopicListener implements RemovalListener<ByteString, TopicStats> {
        @Override
        public void onRemoval(RemovalNotification<ByteString, TopicStats> notification) {
            if (notification.wasEvicted()) {
                logger.info("topic {} is evicted", notification.getKey().toStringUtf8());
                // if the topic is evicted, we need to release the topic.
                releaseTopicInternally(notification.getKey(), false);
            }
        }
    }

    public AbstractTopicManager(ServerConfiguration cfg, ScheduledExecutorService scheduler)
            throws UnknownHostException {
        this.cfg = cfg;
        this.queuer = new TopicOpQueuer(scheduler);
        this.scheduler = scheduler;
        addr = cfg.getServerAddr();

        // build the topic cache
        CacheBuilder<ByteString, TopicStats> cacheBuilder = CacheBuilder.newBuilder()
            .maximumSize(cfg.getMaxNumTopics())
            .initialCapacity(cfg.getInitNumTopics())
            // TODO: change to same number as topic op queuer threads
            .concurrencyLevel(Runtime.getRuntime().availableProcessors())
            .removalListener(new ReleaseTopicListener());
        if (cfg.getRetentionSecsAfterAccess() > 0) {
            cacheBuilder.expireAfterAccess(cfg.getRetentionSecsAfterAccess(), TimeUnit.SECONDS);
        }
        topics = cacheBuilder.build();
    }

    @Override
    public void incrementTopicAccessTimes(ByteString topic) {
        // let guava cache handle hits counting
        topics.getIfPresent(topic);
    }

    @Override
    public synchronized void addTopicOwnershipChangeListener(TopicOwnershipChangeListener listener) {
        listeners.add(listener);
    }

    private void releaseTopicInternally(final ByteString topic, boolean checkExistence) {
        // Enqueue a release operation. (Recall that release
        // doesn't "fail" even if the topic is missing.)
        queuer.pushAndMaybeRun(topic, new ReleaseOp(topic, new Callback<Void>() {

            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                logger.error("failure that should never happen when releasing topic "
                             + topic, exception);
            }

            @Override
            public void operationFinished(Object ctx, Void resultOfOperation) {
                    logger.info("successfully release of topic "
                        + topic.toStringUtf8());
                if (logger.isDebugEnabled()) {
                    logger.debug("successfully release of topic "
                        + topic.toStringUtf8());
                }
            }

        }, null, checkExistence));
    }

    protected final synchronized void notifyListenersAndAddToOwnedTopics(final ByteString topic,
            final Callback<HedwigSocketAddress> originalCallback, final Object originalContext) {

        Callback<Void> postCb = new Callback<Void>() {

            @Override
            public void operationFinished(Object ctx, Void resultOfOperation) {
                topics.put(topic, STUB_TOPIC_STATS);
                if (cfg.getRetentionSecs() > 0) {
                    scheduler.schedule(new Runnable() {
                        @Override
                        public void run() {
                            releaseTopicInternally(topic, true);
                        }
                    }, cfg.getRetentionSecs(), TimeUnit.SECONDS);
                }
                originalCallback.operationFinished(originalContext, addr);
            }

            @Override
            public void operationFailed(final Object ctx, final PubSubException exception) {
                // TODO: optimization: we can release this as soon as we experience the first error.
                Callback<Void> cb = new Callback<Void>() {
                    public void operationFinished(Object _ctx, Void _resultOfOperation) {
                        originalCallback.operationFailed(ctx, exception);
                    }
                    public void operationFailed(Object _ctx, PubSubException _exception) {
                        logger.error("Exception releasing topic", _exception);
                        originalCallback.operationFailed(ctx, exception);
                    }
                };
                
                realReleaseTopic(topic, cb, originalContext);
            }
        };

        Callback<Void> mcb = CallbackUtils.multiCallback(listeners.size(), postCb, null);
        for (TopicOwnershipChangeListener listener : listeners) {
            listener.acquiredTopic(topic, mcb, null);
        }
    }

    private void realReleaseTopic(ByteString topic, Callback<Void> callback, Object ctx) {
        for (TopicOwnershipChangeListener listener : listeners)
            listener.lostTopic(topic);
        topics.invalidate(topic);
        postReleaseCleanup(topic, callback, ctx);
    }

    @Override
    public final void getOwner(ByteString topic, boolean shouldClaim,
                               Callback<HedwigSocketAddress> cb, Object ctx) {
        queuer.pushAndMaybeRun(topic, new GetOwnerOp(topic, shouldClaim, cb, ctx));
    }

    @Override
    public final void releaseTopic(ByteString topic, Callback<Void> cb, Object ctx) {
        queuer.pushAndMaybeRun(topic, new ReleaseOp(topic, cb, ctx));
    }

    /**
     * This method should "return" the owner of the topic if one has been chosen
     * already. If there is no pre-chosen owner, either this hub or some other
     * should be chosen based on the shouldClaim parameter. If its ends up
     * choosing this hub as the owner, the {@code
     * AbstractTopicManager#notifyListenersAndAddToOwnedTopics(ByteString,
     * OperationCallback, Object)} method must be called.
     *
     */
    protected abstract void realGetOwner(ByteString topic, boolean shouldClaim,
                                         Callback<HedwigSocketAddress> cb, Object ctx);

    /**
     * The method should do any cleanup necessary to indicate to other hubs that
     * this topic has been released
     */
    protected abstract void postReleaseCleanup(ByteString topic, Callback<Void> cb, Object ctx);

    @Override
    public void stop() {
        // do nothing now
    }
}
