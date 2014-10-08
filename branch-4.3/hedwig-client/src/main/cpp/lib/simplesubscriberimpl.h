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
#ifndef SIMPLE_SUBSCRIBE_IMPL_H
#define SIMPLE_SUBSCRIBE_IMPL_H

#include <boost/thread/mutex.hpp>

#include "subscriberimpl.h"
#include "clientimpl.h"

namespace Hedwig {

  class SimpleActiveSubscriber : public ActiveSubscriber {
  public:
    SimpleActiveSubscriber(const PubSubDataPtr& data,
                           const AbstractDuplexChannelPtr& channel,
                           const SubscriptionPreferencesPtr& preferences,
                           const DuplexChannelManagerPtr& channelManager);

  protected:
    virtual void doStartDelivery(const MessageHandlerCallbackPtr& handler,
                                 const ClientMessageFilterPtr& filter);

    // Stop Delivery
    virtual void doStopDelivery();
              
    // Queue message when message handler is not ready
    virtual void queueMessage(const PubSubResponsePtr& m);

  private:
    std::size_t maxQueueLen;
  };

  class CloseSubscriptionCallback : public OperationCallback {
  public:
    explicit CloseSubscriptionCallback(const ActiveSubscriberPtr& activeSubscriber,
                                       const SubscriptionEvent event);

    virtual void operationComplete();
    virtual void operationFailed(const std::exception& exception);
  private:
    void finish();
    const ActiveSubscriberPtr activeSubscriber;
    const SubscriptionEvent event;
  };

  // Simple Subscription Channel Handler : One subscription per channel
  class SimpleSubscriberClientChannelHandler : public SubscriberClientChannelHandler {
  public:
    SimpleSubscriberClientChannelHandler(const DuplexChannelManagerPtr& channelManager,
                                         ResponseHandlerMap& handlers);
    virtual ~SimpleSubscriberClientChannelHandler() {}

    // Set the subscriber serving on this channel
    bool setActiveSubscriber(const PubSubDataPtr& op,
                             const SubscriptionPreferencesPtr& preferences);

    virtual void handleSubscriptionEvent(const TopicSubscriber& ts,
                                         const SubscriptionEvent event);

    // Deliver a received message to given message handler
    virtual void deliverMessage(const TopicSubscriber& ts,
                                const PubSubResponsePtr& m);

    // Start Delivery for a given topic subscriber
    virtual void startDelivery(const TopicSubscriber& ts,
                               const MessageHandlerCallbackPtr& handler,
                               const ClientMessageFilterPtr& filter);

    // Stop Delivery for a given topic subscriber
    virtual void stopDelivery(const TopicSubscriber& ts);

    // Has Subscription on the Channel
    virtual bool hasSubscription(const TopicSubscriber& ts);

    // Close Subscription for a given topic subscriber
    virtual void asyncCloseSubscription(const TopicSubscriber& ts,
                                        const OperationCallbackPtr& callback);

    // Consume message for a given topic subscriber
    virtual void consume(const TopicSubscriber& ts,
                         const MessageSeqId& messageSeqId);

  protected:
    // Subscription channel disconnected: reconnect the subscription channel
    virtual void onChannelDisconnected(const DuplexChannelPtr& channel);

    virtual void closeHandler();

  private:
    inline void clearActiveSubscriber() {
      boost::lock_guard<boost::shared_mutex> lock(subscriber_lock);
      subscriber = ActiveSubscriberPtr();
    }

    inline const ActiveSubscriberPtr& getActiveSubscriber() {
      boost::shared_lock<boost::shared_mutex> lock(subscriber_lock);
      return subscriber;
    }

    ActiveSubscriberPtr subscriber;
    boost::shared_mutex subscriber_lock;
  };

  typedef boost::shared_ptr<SimpleSubscriberClientChannelHandler>
    SimpleSubscriberClientChannelHandlerPtr;

  //
  // Simple Duplex Channel Manager
  //

  class SimpleDuplexChannelManager : public DuplexChannelManager {
  public:
    explicit SimpleDuplexChannelManager(const Configuration& conf);
    virtual ~SimpleDuplexChannelManager();

    bool storeSubscriptionChannelHandler(const TopicSubscriber& ts,
                                         const PubSubDataPtr& txn,
                                         const SimpleSubscriberClientChannelHandlerPtr& handler);

    // Get the subscription channel handler for a given subscription
    virtual SubscriberClientChannelHandlerPtr
            getSubscriptionChannelHandler(const TopicSubscriber& ts);

    // Close subscription for a given subscription
    virtual void asyncCloseSubscription(const TopicSubscriber& ts,
                                        const OperationCallbackPtr& callback);

    virtual void handoverDelivery(const TopicSubscriber& ts,
                                  const MessageHandlerCallbackPtr& handler,
                                  const ClientMessageFilterPtr& filter);

    // start the channel manager
    virtual void start();
    // close the channel manager
    virtual void close();

  protected:
    virtual DuplexChannelPtr getSubscriptionChannel(const TopicSubscriber& ts,
                                                    const bool isResubscribeRequest);

    virtual DuplexChannelPtr getSubscriptionChannel(const HostAddress& addr);

    virtual DuplexChannelPtr createSubscriptionChannel(const HostAddress& addr);

    virtual DuplexChannelPtr storeSubscriptionChannel(const DuplexChannelPtr& ch,
                                                      bool doConnect);

  private:
    const SimpleSubscriberClientChannelHandlerPtr&
    getSimpleSubscriptionChannelHandler(const TopicSubscriber& ts);

    std::tr1::unordered_map<TopicSubscriber, SimpleSubscriberClientChannelHandlerPtr, TopicSubscriberHash > topicsubscriber2handler;
    boost::shared_mutex topicsubscriber2handler_lock;

    // Response Handlers for subscription requests
    ResponseHandlerMap subscriptionHandlers;
  };

  typedef boost::shared_ptr<SimpleDuplexChannelManager> SimpleDuplexChannelManagerPtr;

  // Subscribe Response Handler

  class SimpleSubscribeResponseHandler : public ResponseHandler {
  public:
    explicit SimpleSubscribeResponseHandler(
      const SimpleDuplexChannelManagerPtr& channelManager);

    virtual ~SimpleSubscribeResponseHandler() {}

    virtual void handleResponse(const PubSubResponsePtr& m, const PubSubDataPtr& txn,
                                const DuplexChannelPtr& channel);

  private:
    void handleSuccessResponse(const PubSubResponsePtr& m, const PubSubDataPtr& txn,
                               const SimpleSubscriberClientChannelHandlerPtr& handler);
    const SimpleDuplexChannelManagerPtr sChannelManager;
  };
} /* Namespace Hedwig */

#endif
