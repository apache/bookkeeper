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
#ifndef SUBSCRIBE_IMPL_H
#define SUBSCRIBE_IMPL_H

#include <hedwig/subscribe.h>
#include <hedwig/callback.h>
#include "clientimpl.h"
#include <utility>

#ifdef USE_BOOST_TR1
#include <boost/tr1/memory.hpp>
#else
#include <tr1/memory>
#endif

#include <deque>
#include <iostream>

#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/thread/shared_mutex.hpp>

namespace Hedwig {

  class ActiveSubscriber;
  typedef boost::shared_ptr<ActiveSubscriber> ActiveSubscriberPtr;

  class ConsumeWriteCallback : public OperationCallback {
  public:
    ConsumeWriteCallback(const ActiveSubscriberPtr& activeSubscriber,
                         const PubSubDataPtr& data,
                         int retrywait);
    virtual ~ConsumeWriteCallback();

    void operationComplete();
    void operationFailed(const std::exception& exception);

    static void timerComplete(const ActiveSubscriberPtr& activeSubscriber,
                              const PubSubDataPtr& data,
                              const boost::system::error_code& error);
  private:
    const ActiveSubscriberPtr activeSubscriber;
    const PubSubDataPtr data;
    int retrywait;
  };

  class SubscriberClientChannelHandler;
  typedef boost::shared_ptr<SubscriberClientChannelHandler> SubscriberClientChannelHandlerPtr;

  class SubscriberConsumeCallback : public OperationCallback {
  public:
    SubscriberConsumeCallback(const DuplexChannelManagerPtr& channelManager,
                              const ActiveSubscriberPtr& activeSubscriber,
                              const PubSubResponsePtr& m);
    void operationComplete();
    void operationFailed(const std::exception& exception);
    static void timerComplete(const ActiveSubscriberPtr activeSubscriber,
                              const PubSubResponsePtr m,
                              const boost::system::error_code& error);
  private:
    const DuplexChannelManagerPtr channelManager;
    const ActiveSubscriberPtr activeSubscriber;
    const PubSubResponsePtr m;
  };

  class CloseSubscriptionForUnsubscribeCallback : public OperationCallback {
  public:
    CloseSubscriptionForUnsubscribeCallback(const DuplexChannelManagerPtr& channelManager,
                                            const std::string& topic,
                                            const std::string& subscriberId,
                                            const OperationCallbackPtr& unsubCb);
    virtual void operationComplete();
    virtual void operationFailed(const std::exception& exception);
  private:
    const DuplexChannelManagerPtr channelManager;
    const std::string topic;
    const std::string subscriberId;
    const OperationCallbackPtr unsubCb;
  };

  // A instance handle all actions belongs to a subscription
  class ActiveSubscriber : public boost::enable_shared_from_this<ActiveSubscriber> {
  public:
    ActiveSubscriber(const PubSubDataPtr& data,
                     const AbstractDuplexChannelPtr& channel,
                     const SubscriptionPreferencesPtr& preferences,
                     const DuplexChannelManagerPtr& channelManager);
    virtual ~ActiveSubscriber() {}

    // Get the topic
    const std::string& getTopic() const;

    // Get the subscriber id
    const std::string& getSubscriberId() const;

    inline MessageHandlerCallbackPtr getMessageHandler() const {
      return handler;
    }

    inline const AbstractDuplexChannelPtr& getChannel() const {
      return channel;
    }

    // Deliver a received message
    void deliverMessage(const PubSubResponsePtr& m);

    //
    // Start Delivery. If filter is null, just start delivery w/o filter 
    // otherwise start delivery with the given filter.
    // 
    void startDelivery(const MessageHandlerCallbackPtr& handler,
                       const ClientMessageFilterPtr& filter);

    // Stop Delivery
    virtual void stopDelivery();

    // Consume message
    void consume(const MessageSeqId& messageSeqId);

    // Process Event received from subscription channel
    void processEvent(const std::string &topic, const std::string &subscriberId,
                      const SubscriptionEvent event);

    // handover message delivery to other subscriber
    void handoverDelivery();

    // Is resubscribe required
    inline bool isResubscribeRequired() {
      return origData->getSubscriptionOptions().enableresubscribe();
    }

    // Resubscribe the subscriber
    void resubscribe();

    // Close the ActiveSubscriber
    void close();

    friend std::ostream& operator<<(std::ostream& os, const ActiveSubscriber& subscriber);
  protected:
    // Wait to resubscribe
    void waitToResubscribe();

    void retryTimerComplete(const boost::system::error_code& error);

    // Start Delivery with a message filter
    virtual void doStartDelivery(const MessageHandlerCallbackPtr& handler,
                                 const ClientMessageFilterPtr& filter);

    // Stop Delivery
    virtual void doStopDelivery();

    // Queue message when message handler is not ready
    virtual void queueMessage(const PubSubResponsePtr& m);

    AbstractDuplexChannelPtr channel;

    boost::shared_mutex queue_lock;
    std::deque<PubSubResponsePtr> queue;

  private:
    enum DeliveryState {
      STARTING_DELIVERY,
      STARTED_DELIVERY,
      STOPPED_DELIVERY,
    };

    inline void setDeliveryState(DeliveryState state) {
      {
        boost::lock_guard<boost::shared_mutex> lock(deliverystate_lock);
        deliverystate = state;
      }
    }

    boost::shared_mutex deliverystate_lock;
    DeliveryState deliverystate;

    // Keep original handler and filter to handover when resubscribed
    MessageHandlerCallbackPtr origHandler;
    ClientMessageFilterPtr origFilter;

    MessageHandlerCallbackPtr handler;

    const PubSubDataPtr origData;
    const SubscriptionPreferencesPtr preferences;

    DuplexChannelManagerPtr channelManager;

    // variables used for resubscribe
    bool should_wait;
    typedef boost::shared_ptr<boost::asio::deadline_timer> RetryTimerPtr;
    RetryTimerPtr retryTimer;
  };

  class ResubscribeCallback : public ResponseCallback {
  public:
    explicit ResubscribeCallback(const ActiveSubscriberPtr& activeSubscriber);

    virtual void operationComplete(const ResponseBody & resp);
    virtual void operationFailed(const std::exception& exception);

  private:
    const ActiveSubscriberPtr activeSubscriber;
  };

  class SubscriberClientChannelHandler : public HedwigClientChannelHandler,
      public boost::enable_shared_from_this<SubscriberClientChannelHandler> {
  public:
    SubscriberClientChannelHandler(const DuplexChannelManagerPtr& channelManager,
                                   ResponseHandlerMap& handlers);
    virtual ~SubscriberClientChannelHandler();

    virtual void handleSubscriptionEvent(const TopicSubscriber& ts,
                                         const SubscriptionEvent event) = 0;

    // Deliver a received message to given message handler
    virtual void deliverMessage(const TopicSubscriber& ts,
                                const PubSubResponsePtr& m) = 0;

    //
    // Start Delivery for a given topic subscriber. If the filter is null,
    // start delivery w/o filtering; otherwise start delivery with the
    // given message filter.
    //
    virtual void startDelivery(const TopicSubscriber& ts,
                               const MessageHandlerCallbackPtr& handler,
                               const ClientMessageFilterPtr& filter) = 0;

    // Stop Delivery for a given topic subscriber
    virtual void stopDelivery(const TopicSubscriber& ts) = 0;

    // Has Subscription on the Channel
    virtual bool hasSubscription(const TopicSubscriber& ts) = 0;

    // Close Subscription for a given topic subscriber
    virtual void asyncCloseSubscription(const TopicSubscriber& ts,
                                        const OperationCallbackPtr& callback) = 0;

    // Consume message for a given topic subscriber
    virtual void consume(const TopicSubscriber& ts,
                         const MessageSeqId& messageSeqId) = 0;

    // Message received from the underlying channel
    virtual void messageReceived(const DuplexChannelPtr& channel, const PubSubResponsePtr& m);

    // Bind the underlying channel to the subscription channel handler
    inline void setChannel(const AbstractDuplexChannelPtr& channel) {
      this->channel = channel;
    }

    // Return the underlying channel
    inline const AbstractDuplexChannelPtr& getChannel() const {
      return channel;
    }

  protected:
    // close logic for subscription channel handler
    virtual void doClose();

    // Clean the handler status
    virtual void closeHandler() = 0;

    AbstractDuplexChannelPtr channel;
  };

  class SubscriberImpl : public Subscriber {
  public:
    SubscriberImpl(const DuplexChannelManagerPtr& channelManager);
    ~SubscriberImpl();

    void subscribe(const std::string& topic, const std::string& subscriberId, const SubscribeRequest::CreateOrAttach mode);
    void asyncSubscribe(const std::string& topic, const std::string& subscriberId, const SubscribeRequest::CreateOrAttach mode, const OperationCallbackPtr& callback);
    void subscribe(const std::string& topic, const std::string& subscriberId, const SubscriptionOptions& options);
    void asyncSubscribe(const std::string& topic, const std::string& subscriberId, const SubscriptionOptions& options, const OperationCallbackPtr& callback);
    
    void unsubscribe(const std::string& topic, const std::string& subscriberId);
    void asyncUnsubscribe(const std::string& topic, const std::string& subscriberId, const OperationCallbackPtr& callback);

    void consume(const std::string& topic, const std::string& subscriberId, const MessageSeqId& messageSeqId);

    void startDelivery(const std::string& topic, const std::string& subscriberId,
                       const MessageHandlerCallbackPtr& callback);
    void startDeliveryWithFilter(const std::string& topic, const std::string& subscriberId,
                                 const MessageHandlerCallbackPtr& callback,
                                 const ClientMessageFilterPtr& filter);
    void stopDelivery(const std::string& topic, const std::string& subscriberId);

    bool hasSubscription(const std::string& topic, const std::string& subscriberId);
    void closeSubscription(const std::string& topic, const std::string& subscriberId);
    void asyncCloseSubscription(const std::string& topic, const std::string& subscriberId,
                                const OperationCallbackPtr& callback);

    virtual void addSubscriptionListener(SubscriptionListenerPtr& listener);
    virtual void removeSubscriptionListener(SubscriptionListenerPtr& listener);

  private:
    const DuplexChannelManagerPtr channelManager;
  };

  // Unsubscribe Response Handler

  class UnsubscribeResponseHandler : public ResponseHandler {
  public:
    explicit UnsubscribeResponseHandler(const DuplexChannelManagerPtr& channelManager);
    virtual ~UnsubscribeResponseHandler() {}

    virtual void handleResponse(const PubSubResponsePtr& m, const PubSubDataPtr& txn,
                                const DuplexChannelPtr& channel);
  };

  // CloseSubscription Response Handler 

  class CloseSubscriptionResponseHandler : public ResponseHandler {
  public:
    explicit CloseSubscriptionResponseHandler(const DuplexChannelManagerPtr& channelManager);
    virtual ~CloseSubscriptionResponseHandler() {}

    virtual void handleResponse(const PubSubResponsePtr& m, const PubSubDataPtr& txn,
                                const DuplexChannelPtr& channel);
  };
};

#endif
