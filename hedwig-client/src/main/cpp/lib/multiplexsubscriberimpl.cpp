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
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <log4cxx/logger.h>

#include "multiplexsubscriberimpl.h"
#include "util.h"

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("hedwig."__FILE__));

using namespace Hedwig;

RemoveSubscriptionCallback::RemoveSubscriptionCallback(
  const MultiplexDuplexChannelManagerPtr& channelManager,
  const MultiplexSubscriberClientChannelHandlerPtr& handler,
  const TopicSubscriber& ts, const OperationCallbackPtr& callback)
  : channelManager(channelManager), handler(handler),
    topicSubscriber(ts), callback(callback) {
}

void RemoveSubscriptionCallback::operationComplete(const ResponseBody& resp) {
  handler->removeActiveSubscriber(topicSubscriber);
  channelManager->removeSubscriptionChannelHandler(topicSubscriber, handler);
  callback->operationComplete();
}

void RemoveSubscriptionCallback::operationFailed(const std::exception& exception) {
  callback->operationFailed(exception);
}

MultiplexSubscriberClientChannelHandler::MultiplexSubscriberClientChannelHandler(
    const MultiplexDuplexChannelManagerPtr& channelManager, ResponseHandlerMap& handlers)
    : SubscriberClientChannelHandler(boost::dynamic_pointer_cast<DuplexChannelManager>(channelManager), handlers),
      mChannelManager(channelManager) {
}

void MultiplexSubscriberClientChannelHandler::removeActiveSubscriber(
  const TopicSubscriber& ts) {
  ActiveSubscriberPtr as;
  {
    boost::lock_guard<boost::shared_mutex> lock(subscribers_lock);
    as = activeSubscribers[ts];
    activeSubscribers.erase(ts);
    LOG4CXX_DEBUG(logger, "Removed " << ts << " from channel " << channel.get() << ".");
  }
  if (as.get()) {
    as->close();
  }
}

bool MultiplexSubscriberClientChannelHandler::addActiveSubscriber(
  const PubSubDataPtr& op, const SubscriptionPreferencesPtr& preferences) {
  TopicSubscriber ts(op->getTopic(), op->getSubscriberId());
  boost::lock_guard<boost::shared_mutex> lock(subscribers_lock);
  ActiveSubscriberPtr subscriber = activeSubscribers[ts];
  if (subscriber.get()) {
    // NOTE: it should not happen here, since we had subscribers mapping to
    //       avoid two same topic subscribers alive in a client.
    LOG4CXX_WARN(logger, "Duplicate " << *subscriber << " has been found alive on channel "
                         << channel.get());
    return false;
  }
  subscriber = ActiveSubscriberPtr(new ActiveSubscriber(op, channel, preferences,
                                                        channelManager));
  activeSubscribers[ts] = subscriber;
  return true;
}

void MultiplexSubscriberClientChannelHandler::handleSubscriptionEvent(
  const TopicSubscriber& ts, const SubscriptionEvent event) {
  ActiveSubscriberPtr as = getActiveSubscriber(ts);
  if (!as.get()) {
    LOG4CXX_ERROR(logger, "No Active Subscriber found alive for " << ts
                          << " on channel " << channel.get() << " receiving event "
                          << event);
    return;
  }
  if (!as->isResubscribeRequired() &&
      (TOPIC_MOVED == event || SUBSCRIPTION_FORCED_CLOSED == event)) {
    // topic has moved
    if (TOPIC_MOVED == event) {
      // remove topic mapping
      channelManager->clearHostForTopic(as->getTopic(), getChannel()->getHostAddress());
    }
    // first remove the topic subscriber from current handler
    removeActiveSubscriber(ts);
    // second remove it from the mapping
    mChannelManager->removeSubscriptionChannelHandler(ts,
      boost::dynamic_pointer_cast<MultiplexSubscriberClientChannelHandler>(shared_from_this()));
  }
  as->processEvent(ts.first, ts.second, event);
}

void MultiplexSubscriberClientChannelHandler::deliverMessage(const TopicSubscriber& ts,
                                                             const PubSubResponsePtr& m) {
  ActiveSubscriberPtr as = getActiveSubscriber(ts);
  if (!as.get()) {
    LOG4CXX_ERROR(logger, "No Active Subscriber found alive for " << ts
                          << " on channel " << channel.get());
    return;
  }
  as->deliverMessage(m);
}

void MultiplexSubscriberClientChannelHandler::startDelivery(
  const TopicSubscriber& ts, const MessageHandlerCallbackPtr& handler,
  const ClientMessageFilterPtr& filter) {
  ActiveSubscriberPtr as = getActiveSubscriber(ts);
  if (!as.get()) {
    LOG4CXX_ERROR(logger, "No Active Subscriber found alive for " << ts
                          << " on channel " << channel.get());
    throw NotSubscribedException();
  }
  as->startDelivery(handler, filter);
}

void MultiplexSubscriberClientChannelHandler::stopDelivery(const TopicSubscriber& ts) {
  ActiveSubscriberPtr as = getActiveSubscriber(ts);
  if (!as.get()) {
    LOG4CXX_ERROR(logger, "No Active Subscriber found alive for " << ts
                          << " on channel " << channel.get());
    throw NotSubscribedException();
  }
  as->stopDelivery();
}

bool MultiplexSubscriberClientChannelHandler::hasSubscription(const TopicSubscriber& ts) {
  ActiveSubscriberPtr as = getActiveSubscriber(ts);
  if (!as.get()) {
    return false;
  }
  return ts.first == as->getTopic() && ts.second == as->getSubscriberId();
}

void MultiplexSubscriberClientChannelHandler::asyncCloseSubscription(
  const TopicSubscriber& ts, const OperationCallbackPtr& callback) {
  // just remove the active subscriber
  ActiveSubscriberPtr as = getActiveSubscriber(ts);
  if (!as.get()) {
    LOG4CXX_DEBUG(logger, "No Active Subscriber found for " << ts
                          << " when closing its subscription.");
    mChannelManager->removeSubscriptionChannelHandler(ts,
      boost::dynamic_pointer_cast<MultiplexSubscriberClientChannelHandler>(shared_from_this()));
    callback->operationComplete();
    return;
  }

  RemoveSubscriptionCallback * removeCb =
    new RemoveSubscriptionCallback(
      mChannelManager,
      boost::dynamic_pointer_cast<MultiplexSubscriberClientChannelHandler>(shared_from_this()),
      ts, callback);
  ResponseCallbackPtr respCallback(removeCb);
  PubSubDataPtr data =
    PubSubData::forCloseSubscriptionRequest(channelManager->nextTxnId(),
                                            ts.second, ts.first, respCallback);
  channelManager->submitOp(data);
}

void MultiplexSubscriberClientChannelHandler::consume(const TopicSubscriber& ts,
                                                      const MessageSeqId& messageSeqId) {
  ActiveSubscriberPtr as = getActiveSubscriber(ts);
  if (!as.get()) {
    LOG4CXX_ERROR(logger, "No Active Subscriber found for " << ts
                          << " alive on channel " << channel.get());
    return;
  }
  as->consume(messageSeqId);
}

void MultiplexSubscriberClientChannelHandler::onChannelDisconnected(const DuplexChannelPtr& channel) {
  // Clear the subscription channel
  MultiplexSubscriberClientChannelHandlerPtr that =
    boost::dynamic_pointer_cast<MultiplexSubscriberClientChannelHandler>(shared_from_this());
  // remove the channel from channel manager
  mChannelManager->removeSubscriptionChannelHandler(
    getChannel()->getHostAddress(), that);

  // disconnect all the subscribers alive on this channel

  // make a copy of active subscribers to process event, the size is estimated
  std::vector<ActiveSubscriberPtr> copyofActiveSubscribers(activeSubscribers.size());
  copyofActiveSubscribers.clear();
  {
    boost::lock_guard<boost::shared_mutex> lock(subscribers_lock);
    ActiveSubscriberMap::iterator iter = activeSubscribers.begin();
    for (; iter != activeSubscribers.end(); ++iter) {
      ActiveSubscriberPtr as = iter->second;
      if (as.get()) {
        // clear topic ownership
        mChannelManager->clearHostForTopic(as->getTopic(), channel->getHostAddress());

        if (!as->isResubscribeRequired()) {
          TopicSubscriber ts(as->getTopic(), as->getSubscriberId());
          // remove the subscription handler if no need to resubscribe
          mChannelManager->removeSubscriptionChannelHandler(ts, that);
        }
        // close the active subscriber
        as->close();
        copyofActiveSubscribers.push_back(as);
      }
    }
    activeSubscribers.clear();
  }

  // processEvent would emit subscription event to user's callback
  // so it would be better to not put the logic under a lock.
  std::vector<ActiveSubscriberPtr>::iterator viter = copyofActiveSubscribers.begin();
  for (; viter != copyofActiveSubscribers.end(); ++viter) {
    ActiveSubscriberPtr as = *viter;
    if (as.get()) {
      LOG4CXX_INFO(logger, "Tell " << *as << " his channel " << channel.get()
                           << " is disconnected.");
      as->processEvent(as->getTopic(), as->getSubscriberId(), TOPIC_MOVED);
    }
  }
  copyofActiveSubscribers.clear();
}

void MultiplexSubscriberClientChannelHandler::closeHandler() {
  boost::lock_guard<boost::shared_mutex> lock(subscribers_lock);
  ActiveSubscriberMap::iterator iter = activeSubscribers.begin();
  for (; iter != activeSubscribers.end(); ++iter) {
    ActiveSubscriberPtr as = iter->second;
    if (as.get()) {
      as->close();
      LOG4CXX_DEBUG(logger, "Closed " << *as << ".");
    }
  }
}

//
// Subscribe Response Handler
//
MultiplexSubscribeResponseHandler::MultiplexSubscribeResponseHandler(
  const MultiplexDuplexChannelManagerPtr& channelManager)
  : ResponseHandler(boost::dynamic_pointer_cast<DuplexChannelManager>(channelManager)),
    mChannelManager(channelManager) {
}

void MultiplexSubscribeResponseHandler::handleSuccessResponse(
  const PubSubResponsePtr& m, const PubSubDataPtr& txn,
  const MultiplexSubscriberClientChannelHandlerPtr& handler) {
  // for subscribe request, check whether is any subscription preferences received
  SubscriptionPreferencesPtr preferences;
  if (m->has_responsebody()) {
    const ResponseBody& respBody = m->responsebody();
    if (respBody.has_subscriberesponse()) {
      const SubscribeResponse& resp = respBody.subscriberesponse();
      if (resp.has_preferences()) {
        preferences = SubscriptionPreferencesPtr(new SubscriptionPreferences(resp.preferences()));
      }
    }
  }

  TopicSubscriber ts(txn->getTopic(), txn->getSubscriberId()); 
  if (!mChannelManager->storeSubscriptionChannelHandler(ts, txn, handler)) {
    // found existed subscription channel handler
    if (txn->isResubscribeRequest()) {
      txn->getCallback()->operationFailed(ResubscribeException());
    } else {
      txn->getCallback()->operationFailed(AlreadySubscribedException());
    }
    return;
  }

  // If the subscriber has been alive on this channel
  if (!handler->addActiveSubscriber(txn, preferences)) {
    txn->getCallback()->operationFailed(AlreadySubscribedException());
    return;
  }
  if (m->has_responsebody()) {
    txn->getCallback()->operationComplete(m->responsebody());
  } else {
    txn->getCallback()->operationComplete(ResponseBody());
  }
}

void MultiplexSubscribeResponseHandler::handleResponse(
  const PubSubResponsePtr& m, const PubSubDataPtr& txn,
  const DuplexChannelPtr& channel) {
  if (!txn.get()) {
    LOG4CXX_ERROR(logger, "Invalid transaction recevied from channel " << channel.get());
    return;
  }

  LOG4CXX_DEBUG(logger, "message received with status " << m->statuscode()
                        << " from channel " << channel.get());

  MultiplexSubscriberClientChannelHandlerPtr handler =
    boost::dynamic_pointer_cast<MultiplexSubscriberClientChannelHandler>(channel->getChannelHandler());
  if (!handler.get()) {
    LOG4CXX_ERROR(logger, "No simple subscriber client channel handler found for channel "
                          << channel.get() << ".");
    // No channel handler, but we still need to close the channel
    channel->close();
    txn->getCallback()->operationFailed(NoChannelHandlerException());
    return;
  }

  // we don't close any subscription channel when encountering subscribe failures

  switch (m->statuscode()) {
  case SUCCESS:
    handleSuccessResponse(m, txn, handler);
    break;
  case SERVICE_DOWN:
    txn->getCallback()->operationFailed(ServiceDownException());
    break;
  case CLIENT_ALREADY_SUBSCRIBED:
  case TOPIC_BUSY:
    txn->getCallback()->operationFailed(AlreadySubscribedException());
    break;
  case CLIENT_NOT_SUBSCRIBED:
    txn->getCallback()->operationFailed(NotSubscribedException());
    break;
  case NOT_RESPONSIBLE_FOR_TOPIC:
    redirectRequest(m, txn, channel);
    break;
  default:
    LOG4CXX_ERROR(logger, "Unexpected response " << m->statuscode() << " for " << txn->getTxnId());
    txn->getCallback()->operationFailed(UnexpectedResponseException());
    break;
  }
}

//
// Multiplex Duplex Channel Manager
//
MultiplexDuplexChannelManager::MultiplexDuplexChannelManager(const Configuration& conf)
  : DuplexChannelManager(conf) {
  LOG4CXX_DEBUG(logger, "Created MultiplexDuplexChannelManager " << this);
}

MultiplexDuplexChannelManager::~MultiplexDuplexChannelManager() {
  LOG4CXX_DEBUG(logger, "Destroyed MultiplexDuplexChannelManager " << this);
}

void MultiplexDuplexChannelManager::start() {
  // Add subscribe response handler
  subscriptionHandlers[SUBSCRIBE] =
    ResponseHandlerPtr(new MultiplexSubscribeResponseHandler(
      boost::dynamic_pointer_cast<MultiplexDuplexChannelManager>(shared_from_this())));
  subscriptionHandlers[CLOSESUBSCRIPTION] =
    ResponseHandlerPtr(new CloseSubscriptionResponseHandler(shared_from_this()));
  DuplexChannelManager::start();
}

void MultiplexDuplexChannelManager::close() {
  DuplexChannelManager::close();
  subscriptionHandlers.clear();
}

SubscriberClientChannelHandlerPtr
MultiplexDuplexChannelManager::getSubscriptionChannelHandler(const TopicSubscriber& ts) {
  boost::shared_lock<boost::shared_mutex> lock(subscribers_lock);
  return boost::dynamic_pointer_cast<SubscriberClientChannelHandler>(subscribers[ts]);
}

DuplexChannelPtr MultiplexDuplexChannelManager::getSubscriptionChannel(
  const TopicSubscriber& ts, const bool /*isResubscribeRequest*/) {
  const HostAddress& addr = getHostForTopic(ts.first);
  if (addr.isNullHost()) {
    return DuplexChannelPtr();
  } else {
    // we had known which hub server owned the topic
    DuplexChannelPtr ch = getSubscriptionChannel(addr);
    if (ch.get()) {
      return ch;
    }
    ch = createSubscriptionChannel(addr);
    return storeSubscriptionChannel(ch, true);
  }
}

DuplexChannelPtr MultiplexDuplexChannelManager::getSubscriptionChannel(const HostAddress& addr) {
  MultiplexSubscriberClientChannelHandlerPtr handler;
  {
    boost::shared_lock<boost::shared_mutex> lock(subhandlers_lock);
    handler = subhandlers[addr];
  }
  if (handler.get()) {
    return boost::dynamic_pointer_cast<DuplexChannel>(handler->getChannel()); 
  } else {
    return DuplexChannelPtr();
  }
}

DuplexChannelPtr MultiplexDuplexChannelManager::createSubscriptionChannel(const HostAddress& addr) {
  // Create a multiplex subscriber channel handler
  MultiplexSubscriberClientChannelHandler * subscriberHandler =
    new MultiplexSubscriberClientChannelHandler(
      boost::dynamic_pointer_cast<MultiplexDuplexChannelManager>(shared_from_this()),
      subscriptionHandlers);
  ChannelHandlerPtr channelHandler(subscriberHandler);
  // Create a subscription channel
  DuplexChannelPtr channel = createChannel(dispatcher->getService(), addr, channelHandler);
  subscriberHandler->setChannel(boost::dynamic_pointer_cast<AbstractDuplexChannel>(channel));
  LOG4CXX_INFO(logger, "New multiplex subscription channel " << channel.get() << " is created to host "
                       << addr << ", whose channel handler is " << subscriberHandler);
  return channel;
}

DuplexChannelPtr MultiplexDuplexChannelManager::storeSubscriptionChannel(
  const DuplexChannelPtr& ch, bool doConnect) {

  const HostAddress& host = ch->getHostAddress();
  MultiplexSubscriberClientChannelHandlerPtr handler =
    boost::dynamic_pointer_cast<MultiplexSubscriberClientChannelHandler>(ch->getChannelHandler());

  bool useOldCh;
  MultiplexSubscriberClientChannelHandlerPtr oldHandler;
  DuplexChannelPtr oldChannel;
  {
    boost::lock_guard<boost::shared_mutex> lock(subhandlers_lock);

    oldHandler = subhandlers[host];
    if (!oldHandler.get()) {
      subhandlers[host] = handler;
      useOldCh = false;
    } else {
      oldChannel = boost::dynamic_pointer_cast<DuplexChannel>(oldHandler->getChannel());
      useOldCh = true;
    }
  }
  if (useOldCh) {
    LOG4CXX_DEBUG(logger, "Subscription channel " << oldChannel.get() << " with handler "
                          << oldHandler.get() << " was used to serve subscribe requests to host "
                          << host << " so close new channel " << ch.get() << " with handler "
                          << handler.get() << ".");
    handler->close();
    return oldChannel;
  } else {
    if (doConnect) {
      ch->connect();
    }
    LOG4CXX_DEBUG(logger, "Storing channel " << ch.get() << " with handler " << handler.get()
                          << " for host " << host << ".");
    return ch;
  }

}

bool MultiplexDuplexChannelManager::removeSubscriptionChannelHandler(
  const TopicSubscriber& ts, const MultiplexSubscriberClientChannelHandlerPtr& handler) {
  boost::lock_guard<boost::shared_mutex> lock(subscribers_lock);
  MultiplexSubscriberClientChannelHandlerPtr existedHandler = subscribers[ts];
  if (existedHandler.get() == handler.get()) {
    subscribers.erase(ts);
    return true;
  } else {
    return false;
  }
}

bool MultiplexDuplexChannelManager::removeSubscriptionChannelHandler(
  const HostAddress& addr, const MultiplexSubscriberClientChannelHandlerPtr& handler) {
  bool removed;
  {
    boost::lock_guard<boost::shared_mutex> lock(subhandlers_lock);
    MultiplexSubscriberClientChannelHandlerPtr existedHandler = subhandlers[addr];
    if (existedHandler.get() == handler.get()) {
      subhandlers.erase(addr);
      removed = true;
    } else {
      removed = false;
    }
  }
  if (removed && handler.get()) {
    handler->close();
  }
  return removed;
}

bool MultiplexDuplexChannelManager::storeSubscriptionChannelHandler(
  const TopicSubscriber& ts, const PubSubDataPtr& txn,
  const MultiplexSubscriberClientChannelHandlerPtr& handler) {
  MultiplexSubscriberClientChannelHandlerPtr other;
  bool success = false;
  bool isResubscribeRequest = txn->isResubscribeRequest();
  {
    boost::lock_guard<boost::shared_mutex> lock(subscribers_lock);
    other = subscribers[ts];
    if (other.get()) {
      if (isResubscribeRequest) {
        DuplexChannelPtr& origChannel = txn->getOrigChannelForResubscribe();
        const AbstractDuplexChannelPtr& otherChannel = other->getChannel();
        if (origChannel.get() != otherChannel.get()) {
          // channel has been changed for a specific subscriber
          // which means the client closesub and subscribe again
          // when channel disconnect to resubscribe for it.
          // so we should not let the resubscribe succeed
          success = false;
        } else {
          subscribers[ts] = handler;
          success = true;
        }
      } else {
        success = false;
      }
    } else {
      if (isResubscribeRequest) {
        // if it is a resubscribe request and there is no handler found
        // which means a closesub has been called when resubscribing
        // so we should not let the resubscribe succeed
        success = false;
      } else {
        subscribers[ts] = handler;
        success = true;
      }
    }
  }
  return success;
}

void MultiplexDuplexChannelManager::asyncCloseSubscription(
  const TopicSubscriber& ts, const OperationCallbackPtr& callback) {
  SubscriberClientChannelHandlerPtr handler =
    getSubscriptionChannelHandler(ts);
  if (!handler.get()) {
    LOG4CXX_DEBUG(logger, "No subscription channel handler found for " << ts << ".");
    callback->operationComplete();
    return;
  }
  handler->asyncCloseSubscription(ts, callback);
}

void MultiplexDuplexChannelManager::handoverDelivery(
  const TopicSubscriber& ts, const MessageHandlerCallbackPtr& msgHandler,
  const ClientMessageFilterPtr& filter) {
  SubscriberClientChannelHandlerPtr handler =
    getSubscriptionChannelHandler(ts);
  if (!handler.get()) {
    LOG4CXX_WARN(logger, "No subscription channel handler found for " << ts
                         << " to handover delivery with handler "
                         << msgHandler.get() << ", filter " << filter.get() << ".");
    return;
  }
  try {
    handler->startDelivery(ts, msgHandler, filter);
  } catch(const AlreadyStartDeliveryException& ase) {
    LOG4CXX_WARN(logger, "Other one has started delivery for " << ts << " using brand new message handler. "
                         << "It is OK that we could give up handing over old message handler.");
  } catch(const std::exception& e) {
    LOG4CXX_WARN(logger, "Error when handing over old message handler for " << ts
                         << " : " << e.what());
  }
}
