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

#ifndef HEDWIG_CLIENT_IMPL_H
#define HEDWIG_CLIENT_IMPL_H

#include <hedwig/client.h>
#include <hedwig/protocol.h>

#include <boost/asio.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#ifdef USE_BOOST_TR1
#include <boost/tr1/unordered_map.hpp>
#else
#include <tr1/unordered_map>
#endif

#include <list>

#include "util.h"
#include "channel.h"
#include "data.h"
#include "eventdispatcher.h"

namespace Hedwig {
  const int DEFAULT_SYNC_REQUEST_TIMEOUT = 5000;

  template<class R>
  class SyncCallback : public Callback<R> {
  public:
    SyncCallback(int timeout) : response(PENDING), timeout(timeout) {}
    virtual void operationComplete(const R& r) {
      if (response == TIMEOUT) {
        return;
      }

      {
        boost::lock_guard<boost::mutex> lock(mut);
        response = SUCCESS;
        result = r;
      }
      cond.notify_all();
    }

    virtual void operationFailed(const std::exception& exception) {
      if (response == TIMEOUT) {
        return;
      }

      {
        boost::lock_guard<boost::mutex> lock(mut);

        if (typeid(exception) == typeid(ChannelConnectException)) {
          response = NOCONNECT;
        } else if (typeid(exception) == typeid(ServiceDownException)) {
          response = SERVICEDOWN;
        } else if (typeid(exception) == typeid(AlreadySubscribedException)) {
          response = ALREADY_SUBSCRIBED;
        } else if (typeid(exception) == typeid(NotSubscribedException)) {
          response = NOT_SUBSCRIBED;
        } else {
          response = UNKNOWN;
        }
      }
      cond.notify_all();
    }

    void wait() {
      boost::unique_lock<boost::mutex> lock(mut);
      while(response==PENDING) {
        if (cond.timed_wait(lock, boost::posix_time::milliseconds(timeout)) == false) {
          response = TIMEOUT;
        }
      }
    }

    void throwExceptionIfNeeded() {
      switch (response) {
        case SUCCESS:
          break;
        case NOCONNECT:
          throw CannotConnectException();
          break;
        case SERVICEDOWN:
          throw ServiceDownException();
          break;
        case ALREADY_SUBSCRIBED:
          throw AlreadySubscribedException();
          break;
        case NOT_SUBSCRIBED:
          throw NotSubscribedException();
          break;
        case TIMEOUT:
          throw ClientTimeoutException();
          break;
        default:
          throw ClientException();
          break;
      }
    }

    R getResult() { return result; }
    
  private:
    enum { 
      PENDING, 
      SUCCESS,
      NOCONNECT,
      SERVICEDOWN,
      NOT_SUBSCRIBED,
      ALREADY_SUBSCRIBED,
      TIMEOUT,
      UNKNOWN
    } response;

    boost::condition_variable cond;
    boost::mutex mut;
    int timeout;
    R result;
  };

  class SyncOperationCallback : public OperationCallback {
  public:
    SyncOperationCallback(int timeout) : response(PENDING), timeout(timeout) {}
    virtual void operationComplete();
    virtual void operationFailed(const std::exception& exception);
    
    void wait();
    void throwExceptionIfNeeded();
    
  private:
    enum { 
      PENDING, 
      SUCCESS,
      NOCONNECT,
      SERVICEDOWN,
      NOT_SUBSCRIBED,
      ALREADY_SUBSCRIBED,
      TIMEOUT,
      UNKNOWN
    } response;

    boost::condition_variable cond;
    boost::mutex mut;
    int timeout;
  };

  class DuplexChannelManager;
  typedef boost::shared_ptr<DuplexChannelManager> DuplexChannelManagerPtr;

  //
  // Hedwig Response Handler
  //

  // Response Handler used to process response for different types of requests
  class ResponseHandler {
  public:
    ResponseHandler(const DuplexChannelManagerPtr& channelManager); 
    virtual ~ResponseHandler() {};

    virtual void handleResponse(const PubSubResponsePtr& m, const PubSubDataPtr& txn,
                                const DuplexChannelPtr& channel) = 0;
  protected:
    // common method used to redirect request
    void redirectRequest(const PubSubResponsePtr& response, const PubSubDataPtr& data,
                         const DuplexChannelPtr& channel);

    // channel manager to manage all established channels
    const DuplexChannelManagerPtr channelManager;
  };

  typedef boost::shared_ptr<ResponseHandler> ResponseHandlerPtr;
  typedef std::tr1::unordered_map<OperationType, ResponseHandlerPtr, OperationTypeHash> ResponseHandlerMap;

  class PubSubWriteCallback : public OperationCallback {
  public:
    PubSubWriteCallback(const DuplexChannelPtr& channel, const PubSubDataPtr& data); 
    virtual void operationComplete();
    virtual void operationFailed(const std::exception& exception);
  private:
    DuplexChannelPtr channel;
    PubSubDataPtr data;
  };

  class DefaultServerConnectCallback : public OperationCallback {
  public:
    DefaultServerConnectCallback(const DuplexChannelManagerPtr& channelManager,
                                 const DuplexChannelPtr& channel,
                                 const PubSubDataPtr& data);
    virtual void operationComplete();
    virtual void operationFailed(const std::exception& exception);
  private:
    DuplexChannelManagerPtr channelManager;
    DuplexChannelPtr channel;
    PubSubDataPtr data;
  };

  struct SubscriptionListenerPtrHash : public std::unary_function<SubscriptionListenerPtr, size_t> {
    size_t operator()(const Hedwig::SubscriptionListenerPtr& listener) const {
      return reinterpret_cast<size_t>(listener.get());
    }
  };

  // Subscription Event Emitter
  class SubscriptionEventEmitter {
  public:
    SubscriptionEventEmitter();

    void addSubscriptionListener(SubscriptionListenerPtr& listener);
    void removeSubscriptionListener(SubscriptionListenerPtr& listener);
    void emitSubscriptionEvent(const std::string& topic,
                               const std::string& subscriberId,
                               const SubscriptionEvent event);

  private:
    typedef std::tr1::unordered_set<SubscriptionListenerPtr, SubscriptionListenerPtrHash> SubscriptionListenerSet;
    SubscriptionListenerSet listeners;
    boost::shared_mutex listeners_lock;
  };

  class SubscriberClientChannelHandler;

  //
  // Duplex Channel Manager to manage all established channels
  //

  class DuplexChannelManager : public boost::enable_shared_from_this<DuplexChannelManager> {
  public:
    static DuplexChannelManagerPtr create(const Configuration& conf);
    virtual ~DuplexChannelManager();

    inline const Configuration& getConfiguration() const {
      return conf;
    }

    // Submit a pub/sub request
    void submitOp(const PubSubDataPtr& op);

    // Submit a pub/sub request to default host
    // It is called only when client doesn't have the knowledge of topic ownership
    void submitOpToDefaultServer(const PubSubDataPtr& op);

    // Redirect pub/sub request to a target hosts
    void redirectOpToHost(const PubSubDataPtr& op, const HostAddress& host);

    // Submit a pub/sub request thru established channel
    // It is called when connecting to default server to established a channel
    void submitOpThruChannel(const PubSubDataPtr& op, const DuplexChannelPtr& channel);

    // Generate next transaction id for pub/sub requests sending thru this manager
    long nextTxnId();

    // return default host
    inline const HostAddress getDefaultHost() {
      return HostAddress::fromString(defaultHostAddress);
    }

    // set the owner host of a topic
    void setHostForTopic(const std::string& topic, const HostAddress& host);

    // clear all topics that hosted by a hub server
    void clearAllTopicsForHost(const HostAddress& host);

    // clear host for a given topic
    void clearHostForTopic(const std::string& topic, const HostAddress& host);

    // Called when a channel is disconnected
    void nonSubscriptionChannelDied(const DuplexChannelPtr& channel);

    // Remove a channel from all channel map
    void removeChannel(const DuplexChannelPtr& channel);

    // Get the subscription channel handler for a given subscription
    virtual boost::shared_ptr<SubscriberClientChannelHandler>
            getSubscriptionChannelHandler(const TopicSubscriber& ts) = 0;

    // Close subscription for a given subscription
    virtual void asyncCloseSubscription(const TopicSubscriber& ts,
                                        const OperationCallbackPtr& callback) = 0;

    virtual void handoverDelivery(const TopicSubscriber& ts,
                                  const MessageHandlerCallbackPtr& handler,
                                  const ClientMessageFilterPtr& filter) = 0;

    // start the channel manager
    virtual void start();
    // close the channel manager
    virtual void close();
    // whether the channel manager is closed
    bool isClosed();

    // Return an available service
    inline boost::asio::io_service & getService() const {
      return dispatcher->getService()->getService();
    }

    // Return the event emitter
    inline SubscriptionEventEmitter& getEventEmitter() {
      return eventEmitter;
    }

  protected:
    DuplexChannelManager(const Configuration& conf);

    // Get the ownership for a given topic.
    const HostAddress& getHostForTopic(const std::string& topic);

    //
    // Channel Management
    //

    // Non subscription channel management

    // Get a non subscription channel for a given topic
    // If the topic's owner is known, retrieve a subscription channel to
    // target host (if there is no channel existed, create one);
    // If the topic's owner is unknown, return null
    DuplexChannelPtr getNonSubscriptionChannel(const std::string& topic);

    // Get an existed non subscription channel to a given host
    DuplexChannelPtr getNonSubscriptionChannel(const HostAddress& addr);

    // Create a non subscription channel to a given host
    DuplexChannelPtr createNonSubscriptionChannel(const HostAddress& addr);

    // Store the established non subscription channel
    DuplexChannelPtr storeNonSubscriptionChannel(const DuplexChannelPtr& ch,
                                                 bool doConnect); 

    //
    // Subscription Channel Management
    //

    // Get a subscription channel for a given subscription.
    // If there is subscription channel established before, return it.
    // Otherwise, check whether the topic's owner is known. If the topic owner
    // is known, retrieve a subscription channel to target host (if there is no
    // channel exsited, create one); If unknown, return null
    virtual DuplexChannelPtr getSubscriptionChannel(const TopicSubscriber& ts,
                                                    const bool isResubscribeRequest) = 0;

    // Get an existed subscription channel to a given host
    virtual DuplexChannelPtr getSubscriptionChannel(const HostAddress& addr) = 0;

    // Create a subscription channel to a given host
    // If store is true, store the channel for future usage.
    // If store is false, return a newly created channel.
    virtual DuplexChannelPtr createSubscriptionChannel(const HostAddress& addr) = 0;

    // Store the established subscription channel
    virtual DuplexChannelPtr storeSubscriptionChannel(const DuplexChannelPtr& ch,
                                                      bool doConnect) = 0;

    //
    // Raw Channel Management
    //

    // Create a raw channel
    DuplexChannelPtr createChannel(IOServicePtr& service,
                                   const HostAddress& addr, const ChannelHandlerPtr& handler);

    // event dispatcher running io threads
    typedef boost::shared_ptr<EventDispatcher> EventDispatcherPtr;
    EventDispatcherPtr dispatcher;

    // topic2host mapping for topic ownership
    std::tr1::unordered_map<std::string, HostAddress> topic2host;
    boost::shared_mutex topic2host_lock;
    typedef std::tr1::unordered_set<std::string> TopicSet;
    typedef boost::shared_ptr<TopicSet> TopicSetPtr;
    typedef std::tr1::unordered_map<HostAddress, TopicSetPtr, HostAddressHash> Host2TopicsMap;
    Host2TopicsMap host2topics;
    boost::shared_mutex host2topics_lock;
  private:
    // write the request to target channel
    void submitTo(const PubSubDataPtr& op, const DuplexChannelPtr& channel);

    const Configuration& conf;
    bool sslEnabled;
    SSLContextFactoryPtr sslCtxFactory;

    // whether the channel manager is shutting down
    bool closed;

    // counter used for generating transaction ids
    ClientTxnCounter counterobj;

    std::string defaultHostAddress;

    // non-subscription channels
    std::tr1::unordered_map<HostAddress, DuplexChannelPtr, HostAddressHash > host2channel;
    boost::shared_mutex host2channel_lock;

    // maintain all established channels
    typedef std::tr1::unordered_set<DuplexChannelPtr, DuplexChannelPtrHash > ChannelMap;
    ChannelMap allchannels;
    boost::shared_mutex allchannels_lock;

    // Response Handlers for non-subscription requests
    ResponseHandlerMap nonSubscriptionHandlers;

    // Subscription Event Emitter
    SubscriptionEventEmitter eventEmitter;
  };

  //
  // Hedwig Client Channel Handler to handle responses received from the channel
  //

  class HedwigClientChannelHandler : public ChannelHandler {
  public:
    HedwigClientChannelHandler(const DuplexChannelManagerPtr& channelManager,
                               ResponseHandlerMap& handlers);
    virtual ~HedwigClientChannelHandler() {}
    
    virtual void messageReceived(const DuplexChannelPtr& channel, const PubSubResponsePtr& m);
    virtual void channelConnected(const DuplexChannelPtr& channel);
    virtual void channelDisconnected(const DuplexChannelPtr& channel, const std::exception& e);
    virtual void exceptionOccurred(const DuplexChannelPtr& channel, const std::exception& e);

    void close();
  protected:
    // real channel disconnected logic
    virtual void onChannelDisconnected(const DuplexChannelPtr& channel);

    // real close logic
    virtual void doClose();

    // channel manager to manage all established channels
    const DuplexChannelManagerPtr channelManager;
    ResponseHandlerMap& handlers;

    boost::shared_mutex close_lock;
    // Boolean indicating if we closed the handler explicitly or not.
    // If so, we do not need to do the channel disconnected logic here.
    bool closed;
    // whether channel is disconnected.
    bool disconnected;
  };
  
  class PublisherImpl;
  class SubscriberImpl;

  /**
     Implementation of the hedwig client. This class takes care of globals such as the topic->host map and the transaction id counter.
  */
  class ClientImpl : public boost::enable_shared_from_this<ClientImpl> {
  public:
    static ClientImplPtr Create(const Configuration& conf);
    void Destroy();

    Subscriber& getSubscriber();
    Publisher& getPublisher();

    SubscriberImpl& getSubscriberImpl();
    PublisherImpl& getPublisherImpl();

    ~ClientImpl();
  private:
    ClientImpl(const Configuration& conf);

    const Configuration& conf;

    boost::mutex publishercreate_lock;
    PublisherImpl* publisher;

    boost::mutex subscribercreate_lock;
    SubscriberImpl* subscriber;

    // channel manager manage all channels for the client
    DuplexChannelManagerPtr channelManager;
  };
};
#endif
