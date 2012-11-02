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
#ifndef MULTIPLEX_SUBSCRIBE_IMPL_H
#define MULTIPLEX_SUBSCRIBE_IMPL_H

#include <boost/thread/mutex.hpp>

#include "subscriberimpl.h"
#include "clientimpl.h"

namespace Hedwig {

  class MultiplexDuplexChannelManager;
  typedef boost::shared_ptr<MultiplexDuplexChannelManager> MultiplexDuplexChannelManagerPtr;

  // Multiplex Subscription Channel Handler : multiple subscription per channel
  class MultiplexSubscriberClientChannelHandler : public SubscriberClientChannelHandler {
  public:
    MultiplexSubscriberClientChannelHandler(const MultiplexDuplexChannelManagerPtr& channelManager,
                                            ResponseHandlerMap& handlers);
    virtual ~MultiplexSubscriberClientChannelHandler() {}

    // remove a given topic subscriber
    void removeActiveSubscriber(const TopicSubscriber& ts);

    // Add the subscriber serving on this channel
    bool addActiveSubscriber(const PubSubDataPtr& op,
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
    inline const ActiveSubscriberPtr& getActiveSubscriber(const TopicSubscriber& ts) {
      boost::shared_lock<boost::shared_mutex> lock(subscribers_lock);
      return activeSubscribers[ts];
    }

    typedef std::tr1::unordered_map<TopicSubscriber, ActiveSubscriberPtr, TopicSubscriberHash>
    ActiveSubscriberMap;

    ActiveSubscriberMap activeSubscribers;
    boost::shared_mutex subscribers_lock;

    const MultiplexDuplexChannelManagerPtr mChannelManager;
  };

  typedef boost::shared_ptr<MultiplexSubscriberClientChannelHandler>
    MultiplexSubscriberClientChannelHandlerPtr;

  //
  // Multiplex Duplex Channel Manager
  //

  class MultiplexDuplexChannelManager : public DuplexChannelManager {
  public:
    explicit MultiplexDuplexChannelManager(const Configuration& conf);
    virtual ~MultiplexDuplexChannelManager();

    bool storeSubscriptionChannelHandler(
      const TopicSubscriber& ts, const PubSubDataPtr& txn,
      const MultiplexSubscriberClientChannelHandlerPtr& handler);

    bool removeSubscriptionChannelHandler(
      const TopicSubscriber& ts,
      const MultiplexSubscriberClientChannelHandlerPtr& handler);

    bool removeSubscriptionChannelHandler(
      const HostAddress& addr,
      const MultiplexSubscriberClientChannelHandlerPtr& handler);

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
    std::tr1::unordered_map<HostAddress, MultiplexSubscriberClientChannelHandlerPtr, HostAddressHash> subhandlers;
    boost::shared_mutex subhandlers_lock;
    // A inverse mapping for all available topic subscribers
    std::tr1::unordered_map<TopicSubscriber, MultiplexSubscriberClientChannelHandlerPtr, TopicSubscriberHash> subscribers;
    boost::shared_mutex subscribers_lock;

    // Response Handlers for subscription requests
    ResponseHandlerMap subscriptionHandlers;
  };

  // Subscribe Response Handler

  class MultiplexSubscribeResponseHandler : public ResponseHandler {
  public:
    explicit MultiplexSubscribeResponseHandler(const MultiplexDuplexChannelManagerPtr& channelManager);

    virtual ~MultiplexSubscribeResponseHandler() {}

    virtual void handleResponse(const PubSubResponsePtr& m, const PubSubDataPtr& txn,
                                const DuplexChannelPtr& channel);

  private:
    void handleSuccessResponse(const PubSubResponsePtr& m, const PubSubDataPtr& txn,
                               const MultiplexSubscriberClientChannelHandlerPtr& handler);
    const MultiplexDuplexChannelManagerPtr mChannelManager;
  };

  // Callback delegation to remove subscription from a channel

  class RemoveSubscriptionCallback : public ResponseCallback {
  public:
    explicit RemoveSubscriptionCallback(
      const MultiplexDuplexChannelManagerPtr& channelManager,
      const MultiplexSubscriberClientChannelHandlerPtr& handler,
      const TopicSubscriber& ts, const OperationCallbackPtr& callback);

    virtual void operationComplete(const ResponseBody& response);
    virtual void operationFailed(const std::exception& exception);
  private:
    const MultiplexDuplexChannelManagerPtr channelManager;
    const MultiplexSubscriberClientChannelHandlerPtr handler;
    const TopicSubscriber topicSubscriber;
    const OperationCallbackPtr callback;
  };


} /* Namespace Hedwig */

#endif
