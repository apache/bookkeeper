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

#include "simplesubscriberimpl.h"
#include "util.h"

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("hedwig."__FILE__));

using namespace Hedwig;

const int DEFAULT_MAX_MESSAGE_QUEUE_SIZE = 10;

SimpleActiveSubscriber::SimpleActiveSubscriber(const PubSubDataPtr& data,
                                               const AbstractDuplexChannelPtr& channel,
                                               const SubscriptionPreferencesPtr& preferences,
                                               const DuplexChannelManagerPtr& channelManager)
  : ActiveSubscriber(data, channel, preferences, channelManager) {
  maxQueueLen = channelManager->getConfiguration().getInt(Configuration::MAX_MESSAGE_QUEUE_SIZE,
                                                          DEFAULT_MAX_MESSAGE_QUEUE_SIZE);
}

void SimpleActiveSubscriber::doStartDelivery(const MessageHandlerCallbackPtr& handler,
                                             const ClientMessageFilterPtr& filter) {
  ActiveSubscriber::doStartDelivery(handler, filter);
  // put channel#startReceiving out of lock of subscriber#queue_lock
  // otherwise we enter dead lock
  // subscriber#startDelivery(subscriber#queue_lock) =>
  // channel#startReceiving(channel#receiving_lock) =>
  channel->startReceiving();
}

void SimpleActiveSubscriber::doStopDelivery() {
  channel->stopReceiving();
}

void SimpleActiveSubscriber::queueMessage(const PubSubResponsePtr& m) {
  ActiveSubscriber::queueMessage(m);

  if (queue.size() >= maxQueueLen) {
    channel->stopReceiving();
  }
}

CloseSubscriptionCallback::CloseSubscriptionCallback(const ActiveSubscriberPtr& activeSubscriber,
                                                     const SubscriptionEvent event)
  : activeSubscriber(activeSubscriber), event(event) {
}

void CloseSubscriptionCallback::operationComplete() {
  finish();
}

void CloseSubscriptionCallback::operationFailed(const std::exception& e) {
  finish();
}

void CloseSubscriptionCallback::finish() {
  // Process the disconnect logic after cleaning up
  activeSubscriber->processEvent(activeSubscriber->getTopic(),
                                 activeSubscriber->getSubscriberId(),
                                 event);
}

SimpleSubscriberClientChannelHandler::SimpleSubscriberClientChannelHandler(
    const DuplexChannelManagerPtr& channelManager, ResponseHandlerMap& handlers)
    : SubscriberClientChannelHandler(channelManager, handlers) {
}

bool SimpleSubscriberClientChannelHandler::setActiveSubscriber(
  const PubSubDataPtr& op, const SubscriptionPreferencesPtr& preferences) {
  boost::lock_guard<boost::shared_mutex> lock(subscriber_lock);
  if (subscriber.get()) {
    LOG4CXX_ERROR(logger, *subscriber << " has been found alive on channel " << channel.get());
    return false;
  }
  subscriber = ActiveSubscriberPtr(new SimpleActiveSubscriber(op, channel, preferences,
                                                              channelManager));
  return true;
}

void SimpleSubscriberClientChannelHandler::handleSubscriptionEvent(
  const TopicSubscriber& ts, const SubscriptionEvent event) {
  ActiveSubscriberPtr as = getActiveSubscriber();
  if (!as.get()) {
    LOG4CXX_ERROR(logger, "No Active Subscriber found alive on channel " << channel.get()
                          << " receiving subscription event " << event);
    return;
  }
  if (!as->isResubscribeRequired() &&
      (TOPIC_MOVED == event || SUBSCRIPTION_FORCED_CLOSED == event)) {
    // topic has moved
    if (TOPIC_MOVED == event) {
      // remove topic mapping
      channelManager->clearHostForTopic(as->getTopic(), getChannel()->getHostAddress());
    }
    // close subscription to clean status
    OperationCallbackPtr closeCb(new CloseSubscriptionCallback(as, event));
    TopicSubscriber ts(as->getTopic(), as->getSubscriberId());
    channelManager->asyncCloseSubscription(ts, closeCb);
  } else {
    as->processEvent(ts.first, ts.second, event);
  }
}

void SimpleSubscriberClientChannelHandler::deliverMessage(const TopicSubscriber& ts,
                                                          const PubSubResponsePtr& m) {
  ActiveSubscriberPtr as = getActiveSubscriber();
  if (!as.get()) {
    LOG4CXX_ERROR(logger, "No Active Subscriber found alive on channel " << channel.get());
    return;
  }
  as->deliverMessage(m);
}

void SimpleSubscriberClientChannelHandler::startDelivery(const TopicSubscriber& ts,
                                                         const MessageHandlerCallbackPtr& handler,
                                                         const ClientMessageFilterPtr& filter) {
  ActiveSubscriberPtr as = getActiveSubscriber();
  if (!as.get()) {
    LOG4CXX_ERROR(logger, "No Active Subscriber found alive on channel " << channel.get());
    throw NotSubscribedException();
  }
  as->startDelivery(handler, filter);
}

void SimpleSubscriberClientChannelHandler::stopDelivery(const TopicSubscriber& ts) {
  ActiveSubscriberPtr as = getActiveSubscriber();
  if (!as.get()) {
    LOG4CXX_ERROR(logger, "No Active Subscriber found alive on channel " << channel.get());
    throw NotSubscribedException();
  }
  as->stopDelivery();
}

bool SimpleSubscriberClientChannelHandler::hasSubscription(const TopicSubscriber& ts) {
  ActiveSubscriberPtr as = getActiveSubscriber();
  if (!as.get()) {
    return false;
  }
  return ts.first == as->getTopic() && ts.second == as->getSubscriberId();
}

void SimpleSubscriberClientChannelHandler::asyncCloseSubscription(
  const TopicSubscriber& ts, const OperationCallbackPtr& callback) {
  // just remove the active subscriber
  ActiveSubscriberPtr as = getActiveSubscriber();
  if (as.get()) {
    as->close();
    clearActiveSubscriber();
  }
  callback->operationComplete();
}

void SimpleSubscriberClientChannelHandler::consume(const TopicSubscriber& ts,
                                                   const MessageSeqId& messageSeqId) {
  ActiveSubscriberPtr as = getActiveSubscriber();
  if (!as.get()) {
    LOG4CXX_ERROR(logger, "No Active Subscriber found alive on channel " << channel.get());
    return;
  }
  as->consume(messageSeqId);
}

void SimpleSubscriberClientChannelHandler::onChannelDisconnected(
  const DuplexChannelPtr& channel) {
  ActiveSubscriberPtr as = getActiveSubscriber();
  if (!as.get()) {
    LOG4CXX_ERROR(logger, "No Active Subscriber found when channel " << channel.get()
                          << " disconnected.");
    // no active subscriber found, but we still need to close the channel
    channelManager->removeChannel(channel);
    return;
  }
  // Clear the topic owner ship
  channelManager->clearHostForTopic(as->getTopic(), channel->getHostAddress());

  // When the channel disconnected, if resubscribe is required, we would just
  // cleanup the old channel when resubscribe succeed.
  // Otherwise, we would cleanup the old channel then notify with a TOPIC_MOVED event
  LOG4CXX_INFO(logger, "Tell " << *as << " his channel " << channel.get() << " is disconnected.");
  if (!as->isResubscribeRequired()) {
    OperationCallbackPtr closeCb(new CloseSubscriptionCallback(as, TOPIC_MOVED));
    TopicSubscriber ts(as->getTopic(), as->getSubscriberId());
    channelManager->asyncCloseSubscription(ts, closeCb);
  } else {
    as->processEvent(as->getTopic(), as->getSubscriberId(), TOPIC_MOVED);
  }
}

void SimpleSubscriberClientChannelHandler::closeHandler() {
  // just remove the active subscriber
  ActiveSubscriberPtr as = getActiveSubscriber();
  if (as.get()) {
    as->close();
    clearActiveSubscriber();
    LOG4CXX_DEBUG(logger, "Closed " << *as << ".");
  }
}

//
// Subscribe Response Handler
//
SimpleSubscribeResponseHandler::SimpleSubscribeResponseHandler(
  const SimpleDuplexChannelManagerPtr& channelManager)
  : ResponseHandler(boost::dynamic_pointer_cast<DuplexChannelManager>(channelManager)),
    sChannelManager(channelManager) {
}

void SimpleSubscribeResponseHandler::handleSuccessResponse(
  const PubSubResponsePtr& m, const PubSubDataPtr& txn,
  const SimpleSubscriberClientChannelHandlerPtr& handler) {
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

  handler->setActiveSubscriber(txn, preferences);
  TopicSubscriber ts(txn->getTopic(), txn->getSubscriberId());
  if (!sChannelManager->storeSubscriptionChannelHandler(ts, txn, handler)) {
    // found existed subscription channel handler
    handler->close();
    if (txn->isResubscribeRequest()) {
      txn->getCallback()->operationFailed(ResubscribeException());
    } else {
      txn->getCallback()->operationFailed(AlreadySubscribedException());
    }
    return;
  }
  if (m->has_responsebody()) {
    txn->getCallback()->operationComplete(m->responsebody());
  } else {
    txn->getCallback()->operationComplete(ResponseBody());
  }
}

void SimpleSubscribeResponseHandler::handleResponse(const PubSubResponsePtr& m,
                                                    const PubSubDataPtr& txn,
                                                    const DuplexChannelPtr& channel) {
  if (!txn.get()) {
    LOG4CXX_ERROR(logger, "Invalid transaction recevied from channel " << channel.get());
    return;
  }

  LOG4CXX_DEBUG(logger, "message received with status " << m->statuscode()
                        << " from channel " << channel.get());

  SimpleSubscriberClientChannelHandlerPtr handler =
    boost::dynamic_pointer_cast<SimpleSubscriberClientChannelHandler>(channel->getChannelHandler());
  if (!handler.get()) {
    LOG4CXX_ERROR(logger, "No simple subscriber client channel handler found for channel "
                          << channel.get() << ".");
    // No channel handler, but we still need to close the channel
    channel->close();
    txn->getCallback()->operationFailed(NoChannelHandlerException());
    return;
  }

  if (SUCCESS != m->statuscode()) {
    // Subscribe request doesn't succeed, we close the handle and its binding channel
    handler->close();
  }

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
// Simple Duplex Channel Manager
//
SimpleDuplexChannelManager::SimpleDuplexChannelManager(const Configuration& conf)
  : DuplexChannelManager(conf) {
  LOG4CXX_DEBUG(logger, "Created SimpleDuplexChannelManager " << this);
}

SimpleDuplexChannelManager::~SimpleDuplexChannelManager() {
  LOG4CXX_DEBUG(logger, "Destroyed SimpleDuplexChannelManager " << this);
}

void SimpleDuplexChannelManager::start() {
  // Add subscribe response handler
  subscriptionHandlers[SUBSCRIBE] =
    ResponseHandlerPtr(new SimpleSubscribeResponseHandler(
    boost::dynamic_pointer_cast<SimpleDuplexChannelManager>(shared_from_this())));
  DuplexChannelManager::start();
}

void SimpleDuplexChannelManager::close() {
  DuplexChannelManager::close();
  subscriptionHandlers.clear();
}

SubscriberClientChannelHandlerPtr
SimpleDuplexChannelManager::getSubscriptionChannelHandler(const TopicSubscriber& ts) {
  return boost::dynamic_pointer_cast<SubscriberClientChannelHandler>(
         getSimpleSubscriptionChannelHandler(ts));
}

const SimpleSubscriberClientChannelHandlerPtr&
SimpleDuplexChannelManager::getSimpleSubscriptionChannelHandler(const TopicSubscriber& ts) {
  boost::shared_lock<boost::shared_mutex> lock(topicsubscriber2handler_lock);
  return topicsubscriber2handler[ts];
}

DuplexChannelPtr SimpleDuplexChannelManager::getSubscriptionChannel(
  const TopicSubscriber& ts, const bool isResubscribeRequest) {
  SimpleSubscriberClientChannelHandlerPtr handler;
  // for resubscribe request, we forced a new subscription channel
  if (!isResubscribeRequest) {
    handler = getSimpleSubscriptionChannelHandler(ts);
  }
  // found a live subscription channel
  if (handler.get()) {
    return boost::dynamic_pointer_cast<DuplexChannel>(handler->getChannel());
  }
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

DuplexChannelPtr SimpleDuplexChannelManager::getSubscriptionChannel(const HostAddress& addr) {
  // for simple subscription channel, we established a new channel each time
  return DuplexChannelPtr();
}

DuplexChannelPtr SimpleDuplexChannelManager::createSubscriptionChannel(const HostAddress& addr) {
  // Create a simple subscriber channel handler
  SimpleSubscriberClientChannelHandler * subscriberHandler =
    new SimpleSubscriberClientChannelHandler(
      boost::dynamic_pointer_cast<SimpleDuplexChannelManager>(shared_from_this()),
      subscriptionHandlers);
  ChannelHandlerPtr channelHandler(subscriberHandler);
  // Create a subscription channel
  DuplexChannelPtr channel = createChannel(dispatcher->getService(), addr, channelHandler);
  subscriberHandler->setChannel(boost::dynamic_pointer_cast<AbstractDuplexChannel>(channel));
  LOG4CXX_INFO(logger, "New subscription channel " << channel.get() << " is created to host "
                       << addr << ", whose channel handler is " << subscriberHandler);
  return channel;
}

DuplexChannelPtr SimpleDuplexChannelManager::storeSubscriptionChannel(const DuplexChannelPtr& ch,
                                                                      bool doConnect) {
  // for simple duplex channel manager
  // we just store subscription channel handler until subscribe successfully
  if (doConnect) {
    ch->connect();
  }
  return ch;
}

bool SimpleDuplexChannelManager::storeSubscriptionChannelHandler(
  const TopicSubscriber& ts, const PubSubDataPtr& txn,
  const SimpleSubscriberClientChannelHandlerPtr& handler) {
  SimpleSubscriberClientChannelHandlerPtr other;
  bool success = false;
  bool isResubscribeRequest = txn->isResubscribeRequest();
  {
    boost::lock_guard<boost::shared_mutex> lock(topicsubscriber2handler_lock);
    other = topicsubscriber2handler[ts];
    if (other.get()) {
      if (isResubscribeRequest) {
        DuplexChannelPtr& origChannel = txn->getOrigChannelForResubscribe();
        const AbstractDuplexChannelPtr& otherChannel =
          other->getChannel();
        if (origChannel.get() != otherChannel.get()) {
          // channel has been changed for a specific subscriber
          // which means the client closesub and subscribe again
          // when channel disconnect to resubscribe for it.
          // so we should not let the resubscribe succeed
          success = false;
        } else {
          topicsubscriber2handler[ts] = handler;
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
        topicsubscriber2handler[ts] = handler;
        success = true;
      }
    }
  }
  if (isResubscribeRequest && success && other.get()) {
    // the old handler is evicted due to resubscribe succeed
    // so it is the time to close the old disconnected channel now
    other->close();
  }
  return success;
}

void SimpleDuplexChannelManager::asyncCloseSubscription(const TopicSubscriber& ts,
                                                        const OperationCallbackPtr& callback) {
  SimpleSubscriberClientChannelHandlerPtr handler;
  {
    boost::lock_guard<boost::shared_mutex> lock(topicsubscriber2handler_lock);
    handler = topicsubscriber2handler[ts];
    topicsubscriber2handler.erase(ts);
    LOG4CXX_DEBUG(logger, "CloseSubscription:: remove subscriber channel handler for (topic:"
                          << ts.first << ", subscriber:" << ts.second << ").");
  }

  if (handler.get() != 0) {
    handler->close();
  }
  callback->operationComplete();
}

void SimpleDuplexChannelManager::handoverDelivery(const TopicSubscriber& ts,
                                                  const MessageHandlerCallbackPtr& msgHandler,
                                                  const ClientMessageFilterPtr& filter) {
  SimpleSubscriberClientChannelHandlerPtr handler;
  {
    boost::shared_lock<boost::shared_mutex> lock(topicsubscriber2handler_lock);
    handler = topicsubscriber2handler[ts];
  }

  if (!handler.get()) {
    LOG4CXX_WARN(logger, "No channel handler found for (topic:" << ts.first << ", subscriber:"
                         << ts.second << ") to handover delivery with handler "
                         << msgHandler.get() << ", filter " << filter.get() << ".");
    return;
  }
  try {
    handler->startDelivery(ts, msgHandler, filter);
  } catch(const AlreadyStartDeliveryException& ase) {
    LOG4CXX_WARN(logger, "Other one has started delivery for (topic:" << ts.first <<
                         ", subscriber:" << ts.second << ") using brand new message handler. "
                         << "It is OK that we could give up handing over old message handler.");
  } catch(const std::exception& e) {
    LOG4CXX_WARN(logger, "Error when handing over old message handler for (topic:" << ts.first
                         << ", subscriber:" << ts.second << ") : " << e.what());
  }
}
