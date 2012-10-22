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
package org.apache.hedwig.server.delivery;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import org.apache.bookkeeper.util.MathUtils;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.filter.ServerMessageFilter;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.ProtocolVersion;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEvent;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.common.UnexpectedError;
import org.apache.hedwig.server.netty.ServerStats;
import org.apache.hedwig.server.persistence.Factory;
import org.apache.hedwig.server.persistence.MapMethods;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.persistence.ScanCallback;
import org.apache.hedwig.server.persistence.ScanRequest;
import org.apache.hedwig.util.Callback;
import static org.apache.hedwig.util.VarArgs.va;

public class FIFODeliveryManager implements Runnable, DeliveryManager {

    protected static final Logger logger = LoggerFactory.getLogger(FIFODeliveryManager.class);

    private static Callback<Void> NOP_CALLBACK = new Callback<Void>() {
        @Override
        public void operationFinished(Object ctx, Void result) {
        }
        @Override
        public void operationFailed(Object ctx, PubSubException exception) {
        }
    };

    protected interface DeliveryManagerRequest {
        public void performRequest();
    }

    /**
     * the main queue that the single-threaded delivery manager works off of
     */
    BlockingQueue<DeliveryManagerRequest> requestQueue = new LinkedBlockingQueue<DeliveryManagerRequest>();

    /**
     * The queue of all subscriptions that are facing a transient error either
     * in scanning from the persistence manager, or in sending to the consumer
     */
    Queue<ActiveSubscriberState> retryQueue =
        new PriorityBlockingQueue<ActiveSubscriberState>(32, new Comparator<ActiveSubscriberState>() {
            @Override
            public int compare(ActiveSubscriberState as1, ActiveSubscriberState as2) {
                long s = as1.lastScanErrorTime - as2.lastScanErrorTime;
                return s > 0 ? 1 : (s < 0 ? -1 : 0);
            }
        });

    /**
     * Stores a mapping from topic to the delivery pointers on the topic. The
     * delivery pointers are stored in a sorted map from seq-id to the set of
     * subscribers at that seq-id
     */
    Map<ByteString, SortedMap<Long, Set<ActiveSubscriberState>>> perTopicDeliveryPtrs;

    /**
     * Mapping from delivery end point to the subscriber state that we are
     * serving at that end point. This prevents us e.g., from serving two
     * subscriptions to the same endpoint
     */
    Map<TopicSubscriber, ActiveSubscriberState> subscriberStates;

    private PersistenceManager persistenceMgr;

    private ServerConfiguration cfg;

    // Boolean indicating if this thread should continue running. This is used
    // when we want to stop the thread during a PubSubServer shutdown.
    protected boolean keepRunning = true;
    private final Thread workerThread;

    public FIFODeliveryManager(PersistenceManager persistenceMgr, ServerConfiguration cfg) {
        this.persistenceMgr = persistenceMgr;
        perTopicDeliveryPtrs = new HashMap<ByteString, SortedMap<Long, Set<ActiveSubscriberState>>>();
        subscriberStates = new HashMap<TopicSubscriber, ActiveSubscriberState>();
        workerThread = new Thread(this, "DeliveryManagerThread");
        this.cfg = cfg;
    }

    public void start() {
        workerThread.start();
    }

    /**
     * ===================================================================== Our
     * usual enqueue function, stop if error because of unbounded queue, should
     * never happen
     *
     */
    protected void enqueueWithoutFailure(DeliveryManagerRequest request) {
        if (!requestQueue.offer(request)) {
            throw new UnexpectedError("Could not enqueue object: " + request + " to delivery manager request queue.");
        }
    }

    /**
     * ====================================================================
     * Public interface of the delivery manager
     */

    /**
     * Tells the delivery manager to start sending out messages for a particular
     * subscription
     *
     * @param topic
     * @param subscriberId
     * @param seqIdToStartFrom
     *            Message sequence-id from where delivery should be started
     * @param endPoint
     *            The delivery end point to which send messages to
     * @param filter
     *            Only messages passing this filter should be sent to this
     *            subscriber
     */
    public void startServingSubscription(ByteString topic, ByteString subscriberId,
                                         SubscriptionPreferences preferences,
                                         MessageSeqId seqIdToStartFrom,
                                         DeliveryEndPoint endPoint, ServerMessageFilter filter) {

        ActiveSubscriberState subscriber = 
            new ActiveSubscriberState(topic, subscriberId,
                                      preferences,
                                      seqIdToStartFrom.getLocalComponent() - 1,
                                      endPoint, filter);

        enqueueWithoutFailure(subscriber);
    }

    public void stopServingSubscriber(ByteString topic, ByteString subscriberId,
                                      SubscriptionEvent event,
                                      Callback<Void> cb, Object ctx) {
        ActiveSubscriberState subState =
            subscriberStates.get(new TopicSubscriber(topic, subscriberId));

        if (subState != null) {
            stopServingSubscriber(subState, event, cb, ctx);
        } else {
            cb.operationFinished(ctx, null);
        }
    }

    /**
     * Due to some error or disconnection or unsusbcribe, someone asks us to
     * stop serving a particular endpoint
     *
     * @param subscriber
     *          Subscriber instance
     * @param event
     *          Subscription event indicates why to stop subscriber.
     * @param cb
     *          Callback after the subscriber is stopped.
     * @param ctx
     *          Callback context
     */
    protected void stopServingSubscriber(ActiveSubscriberState subscriber,
                                         SubscriptionEvent event,
                                         Callback<Void> cb, Object ctx) {
        enqueueWithoutFailure(new StopServingSubscriber(subscriber, event, cb, ctx));
    }

    /**
     * Instructs the delivery manager to backoff on the given subscriber and
     * retry sending after some time
     *
     * @param subscriber
     */

    public void retryErroredSubscriberAfterDelay(ActiveSubscriberState subscriber) {

        subscriber.setLastScanErrorTime(MathUtils.now());

        if (!retryQueue.offer(subscriber)) {
            throw new UnexpectedError("Could not enqueue to delivery manager retry queue");
        }
    }

    public void clearRetryDelayForSubscriber(ActiveSubscriberState subscriber) {
        subscriber.clearLastScanErrorTime();
        if (!retryQueue.offer(subscriber)) {
            throw new UnexpectedError("Could not enqueue to delivery manager retry queue");
        }
        // no request in request queue now
        // issue a empty delivery request to not waiting for polling requests queue
        if (requestQueue.isEmpty()) {
            enqueueWithoutFailure(new DeliveryManagerRequest() {
                    @Override
                    public void performRequest() {
                    // do nothing
                    }
                    });
        }
    }

    @Override
    public void messageConsumed(ByteString topic, ByteString subscriberId,
                                MessageSeqId consumedSeqId) {
        ActiveSubscriberState subState =
            subscriberStates.get(new TopicSubscriber(topic, subscriberId));
        if (null == subState) {
            return;
        }
        subState.messageConsumed(consumedSeqId.getLocalComponent()); 
    }

    /**
     * Instructs the delivery manager to move the delivery pointer for a given
     * subscriber
     *
     * @param subscriber
     * @param prevSeqId
     * @param newSeqId
     */
    public void moveDeliveryPtrForward(ActiveSubscriberState subscriber, long prevSeqId, long newSeqId) {
        enqueueWithoutFailure(new DeliveryPtrMove(subscriber, prevSeqId, newSeqId));
    }

    /*
     * ==========================================================================
     * == End of public interface, internal machinery begins.
     */
    public void run() {
        while (keepRunning) {
            DeliveryManagerRequest request = null;

            try {
                // We use a timeout of 1 second, so that we can wake up once in
                // a while to check if there is something in the retry queue.
                request = requestQueue.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // First retry any subscriptions that had failed and need a retry
            retryErroredSubscribers();

            if (request == null) {
                continue;
            }

            request.performRequest();

        }
    }

    /**
     * Stop method which will enqueue a ShutdownDeliveryManagerRequest.
     */
    public void stop() {
        enqueueWithoutFailure(new ShutdownDeliveryManagerRequest());
    }

    protected void retryErroredSubscribers() {
        long lastInterestingFailureTime = MathUtils.now() - cfg.getScanBackoffPeriodMs();
        ActiveSubscriberState subscriber;

        while ((subscriber = retryQueue.peek()) != null) {
            if (subscriber.getLastScanErrorTime() > lastInterestingFailureTime) {
                // Not enough time has elapsed yet, will retry later
                // Since the queue is fifo, no need to check later items
                return;
            }

            // retry now
            subscriber.deliverNextMessage();
            retryQueue.poll();
        }
    }

    protected void removeDeliveryPtr(ActiveSubscriberState subscriber, Long seqId, boolean isAbsenceOk,
                                     boolean pruneTopic) {

        assert seqId != null;

        // remove this subscriber from the delivery pointers data structure
        ByteString topic = subscriber.getTopic();
        SortedMap<Long, Set<ActiveSubscriberState>> deliveryPtrs = perTopicDeliveryPtrs.get(topic);

        if (deliveryPtrs == null && !isAbsenceOk) {
            throw new UnexpectedError("No delivery pointers found while disconnecting " + "channel for topic:" + topic);
        }

        if(null == deliveryPtrs) {
            return;
        }

        if (!MapMethods.removeFromMultiMap(deliveryPtrs, seqId, subscriber) && !isAbsenceOk) {

            throw new UnexpectedError("Could not find subscriber:" + subscriber + " at the expected delivery pointer");
        }

        if (pruneTopic && deliveryPtrs.isEmpty()) {
            perTopicDeliveryPtrs.remove(topic);
        }

    }

    protected long getMinimumSeqId(ByteString topic) {
        SortedMap<Long, Set<ActiveSubscriberState>> deliveryPtrs = perTopicDeliveryPtrs.get(topic);

        if (deliveryPtrs == null || deliveryPtrs.isEmpty()) {
            return Long.MAX_VALUE - 1;
        }
        return deliveryPtrs.firstKey();
    }

    protected void addDeliveryPtr(ActiveSubscriberState subscriber, Long seqId) {

        // If this topic doesn't exist in the per-topic delivery pointers table,
        // create an entry for it
        SortedMap<Long, Set<ActiveSubscriberState>> deliveryPtrs = MapMethods.getAfterInsertingIfAbsent(
                    perTopicDeliveryPtrs, subscriber.getTopic(), TreeMapLongToSetSubscriberFactory.instance);

        MapMethods.addToMultiMap(deliveryPtrs, seqId, subscriber, HashMapSubscriberFactory.instance);
    }

    public class ActiveSubscriberState implements ScanCallback, DeliveryCallback, DeliveryManagerRequest {

        static final int UNLIMITED = 0;

        ByteString topic;
        ByteString subscriberId;
        long lastLocalSeqIdDelivered;
        boolean connected = true;
        DeliveryEndPoint deliveryEndPoint;
        long lastScanErrorTime = -1;
        long localSeqIdDeliveringNow;
        long lastSeqIdCommunicatedExternally;
        long lastSeqIdConsumedUtil;
        boolean isThrottled = false;
        final int messageWindowSize;
        ServerMessageFilter filter;
        // TODO make use of these variables

        final static int SEQ_ID_SLACK = 10;

        public ActiveSubscriberState(ByteString topic, ByteString subscriberId,
                                     SubscriptionPreferences preferences,
                                     long lastLocalSeqIdDelivered,
                                     DeliveryEndPoint deliveryEndPoint,
                                     ServerMessageFilter filter) {
            this.topic = topic;
            this.subscriberId = subscriberId;
            this.lastLocalSeqIdDelivered = lastLocalSeqIdDelivered;
            this.lastSeqIdConsumedUtil = lastLocalSeqIdDelivered;
            this.deliveryEndPoint = deliveryEndPoint;
            this.filter = filter;
            if (preferences.hasMessageWindowSize()) {
                messageWindowSize = preferences.getMessageWindowSize();
            } else {
                if (FIFODeliveryManager.this.cfg.getDefaultMessageWindowSize() > 0) {
                    messageWindowSize =
                        FIFODeliveryManager.this.cfg.getDefaultMessageWindowSize();
                } else {
                    messageWindowSize = UNLIMITED;
                }
            }
        }

        public void setNotConnected(SubscriptionEvent event) {
            // have closed it.
            if (!isConnected()) {
                return;
            }
            this.connected = false;
            if (null != event &&
                (SubscriptionEvent.TOPIC_MOVED == event ||
                 SubscriptionEvent.SUBSCRIPTION_FORCED_CLOSED == event)) {
                // for we need to close the underlying when topic moved
                // or subscription forced closed.
                deliveryEndPoint.close();
            }
            // uninitialize filter
            this.filter.uninitialize();
        }

        public ByteString getTopic() {
            return topic;
        }

        public long getLastLocalSeqIdDelivered() {
            return lastLocalSeqIdDelivered;
        }

        public long getLastScanErrorTime() {
            return lastScanErrorTime;
        }

        public void setLastScanErrorTime(long lastScanErrorTime) {
            this.lastScanErrorTime = lastScanErrorTime;
        }

        /**
         * Clear the last scan error time so it could be retry immediately.
         */
        protected void clearLastScanErrorTime() {
            this.lastScanErrorTime = -1;
        }

        protected boolean isConnected() {
            return connected;
        }

        protected void messageConsumed(long newSeqIdConsumed) {
            if (newSeqIdConsumed <= lastSeqIdConsumedUtil) {
                return;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Subscriber ({}) moved consumed ptr from {} to {}.",
                             va(this, lastSeqIdConsumedUtil, newSeqIdConsumed));
            }
            lastSeqIdConsumedUtil = newSeqIdConsumed;
            // after updated seq id check whether it still exceed msg limitation
            if (msgLimitExceeded()) {
                return;
            }
            if (isThrottled) {
                isThrottled = false;
                logger.info("Try to wake up subscriber ({}) to deliver messages again : last delivered {}, last consumed {}.",
                            va(this, lastLocalSeqIdDelivered, lastSeqIdConsumedUtil));

                enqueueWithoutFailure(new DeliveryManagerRequest() {
                    @Override
                    public void performRequest() {
                        // enqueue 
                        clearRetryDelayForSubscriber(ActiveSubscriberState.this);            
                    }
                });
            }
        }

        protected boolean msgLimitExceeded() {
            if (messageWindowSize == UNLIMITED) {
                return false;
            }
            if (lastLocalSeqIdDelivered - lastSeqIdConsumedUtil >= messageWindowSize) {
                return true;
            }
            return false;
        }

        public void deliverNextMessage() {

            if (!isConnected()) {
                return;
            }

            // check whether we have delivered enough messages without receiving their consumes
            if (msgLimitExceeded()) {
                logger.info("Subscriber ({}) is throttled : last delivered {}, last consumed {}.",
                            va(this, lastLocalSeqIdDelivered, lastSeqIdConsumedUtil));
                isThrottled = true;
                // do nothing, since the delivery process would be throttled.
                // After message consumed, it would be added back to retry queue.
                return;
            }

            localSeqIdDeliveringNow = persistenceMgr.getSeqIdAfterSkipping(topic, lastLocalSeqIdDelivered, 1);

            ScanRequest scanRequest = new ScanRequest(topic, localSeqIdDeliveringNow,
                    /* callback= */this, /* ctx= */null);

            persistenceMgr.scanSingleMessage(scanRequest);
        }

        /**
         * ===============================================================
         * {@link ScanCallback} methods
         */

        public void messageScanned(Object ctx, Message message) {
            if (!connected) {
                return;
            }

            if (!filter.testMessage(message)) {
                sendingFinished();
                return;
            }

            /**
             * The method below will invoke our sendingFinished() method when
             * done
             */
            PubSubResponse response = PubSubResponse.newBuilder().setProtocolVersion(ProtocolVersion.VERSION_ONE)
                                      .setStatusCode(StatusCode.SUCCESS).setTxnId(0).setMessage(message).build();

            deliveryEndPoint.send(response, //
                                  // callback =
                                  this);

        }

        public void scanFailed(Object ctx, Exception exception) {
            if (!connected) {
                return;
            }

            // wait for some time and then retry
            retryErroredSubscriberAfterDelay(this);
        }

        public void scanFinished(Object ctx, ReasonForFinish reason) {
            // no-op
        }

        /**
         * ===============================================================
         * {@link DeliveryCallback} methods
         */
        public void sendingFinished() {
            if (!connected) {
                return;
            }

            lastLocalSeqIdDelivered = localSeqIdDeliveringNow;

            if (lastLocalSeqIdDelivered > lastSeqIdCommunicatedExternally + SEQ_ID_SLACK) {
                // Note: The order of the next 2 statements is important. We should
                // submit a request to change our delivery pointer only *after* we
                // have actually changed it. Otherwise, there is a race condition
                // with removal of this channel, w.r.t, maintaining the deliveryPtrs
                // tree map.
                long prevId = lastSeqIdCommunicatedExternally;
                lastSeqIdCommunicatedExternally = lastLocalSeqIdDelivered;
                moveDeliveryPtrForward(this, prevId, lastLocalSeqIdDelivered);
            }
            // increment deliveried message
            ServerStats.getInstance().incrementMessagesDelivered();
            deliverNextMessage();
        }

        public long getLastSeqIdCommunicatedExternally() {
            return lastSeqIdCommunicatedExternally;
        }


        public void permanentErrorOnSend() {
            // the underlying channel is broken, the channel will
            // be closed in UmbrellaHandler when exception happened.
            // so we don't need to close the channel again
            stopServingSubscriber(this, null,
                                  NOP_CALLBACK, null);
        }

        public void transientErrorOnSend() {
            retryErroredSubscriberAfterDelay(this);
        }

        /**
         * ===============================================================
         * {@link DeliveryManagerRequest} methods
         */
        public void performRequest() {

            // Put this subscriber in the channel to subscriber mapping
            ActiveSubscriberState prevSubscriber =
                subscriberStates.put(new TopicSubscriber(topic, subscriberId), this);

            if (prevSubscriber != null) {
                stopServingSubscriber(prevSubscriber, SubscriptionEvent.SUBSCRIPTION_FORCED_CLOSED,
                                      NOP_CALLBACK, null);
            }

            lastSeqIdCommunicatedExternally = lastLocalSeqIdDelivered;
            addDeliveryPtr(this, lastLocalSeqIdDelivered);

            deliverNextMessage();
        };

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Topic: ");
            sb.append(topic.toStringUtf8());
            sb.append("DeliveryPtr: ");
            sb.append(lastLocalSeqIdDelivered);
            return sb.toString();

        }
    }

    protected class StopServingSubscriber implements DeliveryManagerRequest {
        ActiveSubscriberState subscriber;
        SubscriptionEvent event;
        final Callback<Void> cb;
        final Object ctx;

        public StopServingSubscriber(ActiveSubscriberState subscriber,
                                     SubscriptionEvent event,
                                     Callback<Void> callback, Object ctx) {
            this.subscriber = subscriber;
            this.event = event;
            this.cb = callback;
            this.ctx = ctx;
        }

        @Override
        public void performRequest() {

            // This will automatically stop delivery, and disconnect the channel
            subscriber.setNotConnected(event);

            // if the subscriber has moved on, a move request for its delivery
            // pointer must be pending in the request queue. Note that the
            // subscriber first changes its delivery pointer and then submits a
            // request to move so this works.
            removeDeliveryPtr(subscriber, subscriber.getLastSeqIdCommunicatedExternally(), //
                              // isAbsenceOk=
                              true,
                              // pruneTopic=
                              true);
            cb.operationFinished(ctx, null);
        }

    }

    protected class DeliveryPtrMove implements DeliveryManagerRequest {

        ActiveSubscriberState subscriber;
        Long oldSeqId;
        Long newSeqId;

        public DeliveryPtrMove(ActiveSubscriberState subscriber, Long oldSeqId, Long newSeqId) {
            this.subscriber = subscriber;
            this.oldSeqId = oldSeqId;
            this.newSeqId = newSeqId;
        }

        @Override
        public void performRequest() {
            ByteString topic = subscriber.getTopic();
            long prevMinSeqId = getMinimumSeqId(topic);

            if (subscriber.isConnected()) {
                removeDeliveryPtr(subscriber, oldSeqId, //
                                  // isAbsenceOk=
                                  false,
                                  // pruneTopic=
                                  false);

                addDeliveryPtr(subscriber, newSeqId);
            } else {
                removeDeliveryPtr(subscriber, oldSeqId, //
                                  // isAbsenceOk=
                                  true,
                                  // pruneTopic=
                                  true);
            }

            long nowMinSeqId = getMinimumSeqId(topic);

            if (nowMinSeqId > prevMinSeqId) {
                persistenceMgr.deliveredUntil(topic, nowMinSeqId);
            }
        }
    }

    protected class ShutdownDeliveryManagerRequest implements DeliveryManagerRequest {
        // This is a simple type of Request we will enqueue when the
        // PubSubServer is shut down and we want to stop the DeliveryManager
        // thread.
        public void performRequest() {
            keepRunning = false;
        }
    }

    /**
     * ====================================================================
     *
     * Dumb factories for our map methods
     */
    protected static class TreeMapLongToSetSubscriberFactory implements
        Factory<SortedMap<Long, Set<ActiveSubscriberState>>> {
        static TreeMapLongToSetSubscriberFactory instance = new TreeMapLongToSetSubscriberFactory();

        @Override
        public SortedMap<Long, Set<ActiveSubscriberState>> newInstance() {
            return new TreeMap<Long, Set<ActiveSubscriberState>>();
        }
    }

    protected static class HashMapSubscriberFactory implements Factory<Set<ActiveSubscriberState>> {
        static HashMapSubscriberFactory instance = new HashMapSubscriberFactory();

        @Override
        public Set<ActiveSubscriberState> newInstance() {
            return new HashSet<ActiveSubscriberState>();
        }
    }

}
