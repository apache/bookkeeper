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

#include <string>
#include <boost/asio.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include <log4cxx/logger.h>

#include "subscriberimpl.h"
#include "util.h"
#include "channel.h"
#include "filterablemessagehandler.h"

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("hedwig."__FILE__));

using namespace Hedwig;
const int DEFAULT_MESSAGE_CONSUME_RETRY_WAIT_TIME = 5000;
const int DEFAULT_SUBSCRIBER_CONSUME_RETRY_WAIT_TIME = 5000;
const int DEFAULT_RECONNECT_SUBSCRIBE_RETRY_WAIT_TIME = 5000;
const bool DEFAULT_SUBSCRIBER_AUTOCONSUME = true;
const int DEFAULT_SUBSCRIPTION_MESSAGE_BOUND = 0;

static const std::type_info& RESUBSCRIBE_EXCEPTION = typeid(ResubscribeException());

ConsumeWriteCallback::ConsumeWriteCallback(const ActiveSubscriberPtr& activeSubscriber,
                                           const PubSubDataPtr& data,
                                           int retrywait)
  : activeSubscriber(activeSubscriber), data(data), retrywait(retrywait) {
}

ConsumeWriteCallback::~ConsumeWriteCallback() {
}

/* static */ void ConsumeWriteCallback::timerComplete(
                  const ActiveSubscriberPtr& activeSubscriber,
                  const PubSubDataPtr& data,
                  const boost::system::error_code& error) {
  if (error) {
    // shutting down
    return;
  }

  activeSubscriber->consume(data->getMessageSeqId());
}

void ConsumeWriteCallback::operationComplete() {
  LOG4CXX_DEBUG(logger, "Successfully wrote consume transaction: " << data->getTxnId());
}

void ConsumeWriteCallback::operationFailed(const std::exception& exception) {
  LOG4CXX_ERROR(logger, "Error writing consume request (topic:" << data->getTopic()
                     << ", subscriber:" << data->getSubscriberId() << ", txn:" << data->getTxnId()
                     << ") : " << exception.what() << ", will be tried in "
                     << retrywait << " milliseconds");

  boost::asio::deadline_timer t(activeSubscriber->getChannel()->getService(),
                                boost::posix_time::milliseconds(retrywait));
}

SubscriberConsumeCallback::SubscriberConsumeCallback(const DuplexChannelManagerPtr& channelManager,
                                                     const ActiveSubscriberPtr& activeSubscriber,
                                                     const PubSubResponsePtr& m)
  : channelManager(channelManager), activeSubscriber(activeSubscriber), m(m) {
}

void SubscriberConsumeCallback::operationComplete() {
  LOG4CXX_DEBUG(logger, "ConsumeCallback::operationComplete " << *activeSubscriber);

  if (channelManager->getConfiguration().getBool(Configuration::SUBSCRIBER_AUTOCONSUME,
                                                 DEFAULT_SUBSCRIBER_AUTOCONSUME)) {
    activeSubscriber->consume(m->message().msgid());
  }
}

/* static */ void SubscriberConsumeCallback::timerComplete(
                 const ActiveSubscriberPtr activeSubscriber,
                 const PubSubResponsePtr m,
                 const boost::system::error_code& error) {
  if (error) {
    return;
  }
  activeSubscriber->deliverMessage(m);
}

void SubscriberConsumeCallback::operationFailed(const std::exception& exception) {
  LOG4CXX_ERROR(logger, "ConsumeCallback::operationFailed  " << *activeSubscriber);

  int retrywait = channelManager->getConfiguration()
                  .getInt(Configuration::SUBSCRIBER_CONSUME_RETRY_WAIT_TIME,
                          DEFAULT_SUBSCRIBER_CONSUME_RETRY_WAIT_TIME);

  LOG4CXX_ERROR(logger, "Error passing message to client for " << *activeSubscriber << " error: "
                        << exception.what() << " retrying in " << retrywait << " Microseconds");

  // We leverage same io service for retrying delivering messages.
  AbstractDuplexChannelPtr ch = activeSubscriber->getChannel();
  boost::asio::deadline_timer t(ch->getService(), boost::posix_time::milliseconds(retrywait));

  t.async_wait(boost::bind(&SubscriberConsumeCallback::timerComplete,
                           activeSubscriber, m, boost::asio::placeholders::error));
}

CloseSubscriptionForUnsubscribeCallback::CloseSubscriptionForUnsubscribeCallback(
  const DuplexChannelManagerPtr& channelManager, const std::string& topic,
  const std::string& subscriberId, const OperationCallbackPtr& unsubCb)
  : channelManager(channelManager), topic(topic), subscriberId(subscriberId), unsubCb(unsubCb) {
}

void CloseSubscriptionForUnsubscribeCallback::operationComplete() {
  ResponseCallbackPtr respCallback(new ResponseCallbackAdaptor(unsubCb));
  PubSubDataPtr data = PubSubData::forUnsubscribeRequest(channelManager->nextTxnId(),
                                                         subscriberId, topic, respCallback);
  // submit the unsubscribe request
  channelManager->submitOp(data);
}

void CloseSubscriptionForUnsubscribeCallback::operationFailed(const std::exception& exception) {
  unsubCb->operationFailed(exception);
}

ResubscribeCallback::ResubscribeCallback(const ActiveSubscriberPtr& activeSubscriber)
  : activeSubscriber(activeSubscriber) {
}

void ResubscribeCallback::operationComplete(const ResponseBody & resp) {
  // handover delivery to resubscribed subscriber.
  activeSubscriber->handoverDelivery();
}

void ResubscribeCallback::operationFailed(const std::exception& exception) {
  if (RESUBSCRIBE_EXCEPTION == typeid(exception)) {
    // it might be caused by closesub when resubscribing.
    // so we don't need to retry resubscribe again
    LOG4CXX_WARN(logger, "Failed to resubscribe " << *activeSubscriber
                         << " : but it is caused by closesub when resubscribing. "
                         << "so we don't need to retry subscribe again.");
    return;
  }
  LOG4CXX_ERROR(logger, "Failed to resubscribe " << *activeSubscriber << ", will retry later : "
                        << exception.what());
  activeSubscriber->resubscribe();
}

ActiveSubscriber::ActiveSubscriber(const PubSubDataPtr& data,
                                   const AbstractDuplexChannelPtr& channel,
                                   const SubscriptionPreferencesPtr& preferences,
                                   const DuplexChannelManagerPtr& channelManager)
  : channel(channel), deliverystate(STOPPED_DELIVERY), origData(data),
    preferences(preferences), channelManager(channelManager), should_wait(false) {
  LOG4CXX_DEBUG(logger, "Creating ActiveSubscriber " << this << " for (topic:"
                        << data->getTopic() << ", subscriber:" << data->getSubscriberId() << ").");
}

const std::string& ActiveSubscriber::getTopic() const {
  return origData->getTopic();
}

const std::string& ActiveSubscriber::getSubscriberId() const {
  return origData->getSubscriberId();
}

void ActiveSubscriber::deliverMessage(const PubSubResponsePtr& m) {
  boost::lock_guard<boost::shared_mutex> lock(queue_lock);

  LOG4CXX_INFO(logger, "Message received (topic:" << origData->getTopic() << ", subscriberId:"
                       << origData->getSubscriberId() << ", msgId:" << m->message().msgid().localcomponent()
                       << ") from channel " << channel.get());

  if (this->handler.get()) {
    OperationCallbackPtr callback(new SubscriberConsumeCallback(channelManager, shared_from_this(), m));
    this->handler->consume(origData->getTopic(), origData->getSubscriberId(), m->message(), callback);
  } else {
    queueMessage(m);
  }
}

void ActiveSubscriber::queueMessage(const PubSubResponsePtr& m) {
  queue.push_back(m);
}

void ActiveSubscriber::startDelivery(const MessageHandlerCallbackPtr& origHandler,
                                     const ClientMessageFilterPtr& origFilter) {
  // check delivery state to avoid dealock when calling startdelivery/stopdelivery
  // in message handler.
  // STOPPED_DELIVERY => STARTED_DELIVERY (only one could start delivery)
  {
    boost::lock_guard<boost::shared_mutex> lock(deliverystate_lock);
    if (STARTED_DELIVERY == deliverystate) {
      LOG4CXX_ERROR(logger, *this << " has started delivery with message handler "
                            << this->handler.get());
      throw AlreadyStartDeliveryException();
    } else if (STARTING_DELIVERY == deliverystate) {
      LOG4CXX_ERROR(logger, *this << " is starting delivery by other one now.");
      throw StartingDeliveryException();
    }
    deliverystate = STARTING_DELIVERY;
  }
  try {
    doStartDelivery(origHandler, origFilter);
    // STARTING_DELIVERY => STARTED_DELIVERY
    setDeliveryState(STARTED_DELIVERY);
  } catch (const std::exception& e) {
    // STARTING_DELIVERY => STOPPED_DELIVERY
    setDeliveryState(STOPPED_DELIVERY); 
    throw e;
  }
}

void ActiveSubscriber::doStartDelivery(const MessageHandlerCallbackPtr& origHandler,
                                       const ClientMessageFilterPtr& origFilter) {
  MessageHandlerCallbackPtr handler;
  // origHandler & origFilter has been passed validation. If origFilter is null,
  // we start delivery w/o message filtering. or If the preferences is null, which
  // means we connected to an old version hub server, also starts w/o message filtering
  if (origFilter.get() && preferences.get()) {
    origFilter->setSubscriptionPreferences(origData->getTopic(), origData->getSubscriberId(),
                                           preferences);
    handler = MessageHandlerCallbackPtr(new FilterableMessageHandler(origHandler, origFilter));
  } else {
    handler = origHandler;
  }
  {
    boost::lock_guard<boost::shared_mutex> lock(queue_lock);

    if (this->handler.get()) {
      LOG4CXX_ERROR(logger, *this << " has started delivery with message handler "
                            << this->handler.get());
      throw AlreadyStartDeliveryException();
    }

    if (!handler.get()) {
      // no message handler callback
      LOG4CXX_WARN(logger, *this << " try to start an empty message handler");
      return;
    }

    this->handler = handler;
    // store the original filter and handler
    this->origHandler = origHandler;
    this->origFilter = origFilter;

    while (!queue.empty()) {
      PubSubResponsePtr m = queue.front();
      queue.pop_front();

      OperationCallbackPtr callback(new SubscriberConsumeCallback(channelManager, shared_from_this(), m));
      this->handler->consume(origData->getTopic(), origData->getSubscriberId(), m->message(), callback);
    }
  }

  LOG4CXX_INFO(logger, *this << " #startDelivery to receive messages from channel " << channel.get());
}

void ActiveSubscriber::stopDelivery() {
  // if someone is starting delivery, we should not allow it to stop.
  // otherwise we would break order gurantee. since queued message would be
  // delivered to message handler when #startDelivery.
  {
    boost::lock_guard<boost::shared_mutex> lock(deliverystate_lock);
    if (STARTING_DELIVERY == deliverystate) {
      LOG4CXX_ERROR(logger, "someone is starting delivery for " << *this
                            << ". we could not stop delivery now.");
      throw StartingDeliveryException();      
    }
  }
  LOG4CXX_INFO(logger, *this << " #stopDelivery to stop receiving messages from channel " << channel.get());
  // actual stop delivery
  doStopDelivery();
  boost::lock_guard<boost::shared_mutex> lock(queue_lock);
  this->handler = MessageHandlerCallbackPtr();
  // marked the state to stopped
  setDeliveryState(STOPPED_DELIVERY);
}

void ActiveSubscriber::doStopDelivery() {
  // do nothing.
}

void ActiveSubscriber::consume(const MessageSeqId& messageSeqId) {
  PubSubDataPtr data = PubSubData::forConsumeRequest(channelManager->nextTxnId(),
                                                     origData->getSubscriberId(),
                                                     origData->getTopic(), messageSeqId);

  int retrywait = channelManager->getConfiguration()
                  .getInt(Configuration::MESSAGE_CONSUME_RETRY_WAIT_TIME,
                          DEFAULT_MESSAGE_CONSUME_RETRY_WAIT_TIME);
  OperationCallbackPtr writecb(new ConsumeWriteCallback(shared_from_this(), data, retrywait));
  channel->writeRequest(data->getRequest(), writecb);
}

void ActiveSubscriber::handoverDelivery() {
  if (handler.get()) {
    TopicSubscriber ts(origData->getTopic(), origData->getSubscriberId());
    // handover the message handler to other active subscriber
    channelManager->handoverDelivery(ts, origHandler, origFilter);
  }
}

void ActiveSubscriber::processEvent(const std::string &topic, const std::string &subscriberId,
                                    const SubscriptionEvent event) {
  if (!isResubscribeRequired()) {
    channelManager->getEventEmitter().emitSubscriptionEvent(topic, subscriberId, event);
    return;
  }
  // resumbit the subscribe request
  switch (event) {
  case TOPIC_MOVED:
  case SUBSCRIPTION_FORCED_CLOSED:
    resubscribe();
    break;
  default:
    LOG4CXX_ERROR(logger, "Received unknown subscription event " << event
                          << " for (topic:" << topic << ", subscriber:" << subscriberId << ").");
    break;
  }
}

void ActiveSubscriber::resubscribe() {
  if (should_wait) {
    waitToResubscribe();
    return;
  }
  should_wait = true;

  origData->clearTriedServers();
  origData->setCallback(ResponseCallbackPtr(new ResubscribeCallback(shared_from_this())));
  DuplexChannelPtr origChannel = 
    boost::dynamic_pointer_cast<DuplexChannel>(channel);
  origData->setOrigChannelForResubscribe(origChannel);

  // submit subscribe request again
  channelManager->submitOp(origData);
}

void ActiveSubscriber::waitToResubscribe() {
  int retrywait = channelManager->getConfiguration().getInt(Configuration::RECONNECT_SUBSCRIBE_RETRY_WAIT_TIME,
                                                            DEFAULT_RECONNECT_SUBSCRIBE_RETRY_WAIT_TIME);
  retryTimer = RetryTimerPtr(new boost::asio::deadline_timer(channel->getService(),
                             boost::posix_time::milliseconds(retrywait)));
  retryTimer->async_wait(boost::bind(&ActiveSubscriber::retryTimerComplete,
                                     shared_from_this(), boost::asio::placeholders::error));
}

void ActiveSubscriber::retryTimerComplete(const boost::system::error_code& error) {
  if (error) {
    return;
  }
  should_wait = false;
  // resubscribe again
  resubscribe();
}

void ActiveSubscriber::close() {
  // cancel reconnect timer
  RetryTimerPtr timer = retryTimer;
  if (timer.get()) {
    boost::system::error_code ec;
    timer->cancel(ec);
    if (ec) {
      LOG4CXX_WARN(logger,  *this << " cancel resubscribe task " << timer.get() << " error :"
                            << ec.message().c_str());
    }
  }
}

SubscriberClientChannelHandler::SubscriberClientChannelHandler(
  const DuplexChannelManagerPtr& channelManager, ResponseHandlerMap& handlers)
  : HedwigClientChannelHandler(channelManager, handlers) {
  LOG4CXX_DEBUG(logger, "Creating SubscriberClientChannelHandler " << this);
}

SubscriberClientChannelHandler::~SubscriberClientChannelHandler() {
  LOG4CXX_DEBUG(logger, "Cleaning up SubscriberClientChannelHandler " << this);
}

void SubscriberClientChannelHandler::messageReceived(const DuplexChannelPtr& channel, const PubSubResponsePtr& m) {
  if (m->has_message()) {
    TopicSubscriber ts(m->topic(), m->subscriberid());
    // dispatch the message to target topic subscriber.
    deliverMessage(ts, m);
    return;
  }
  if (m->has_responsebody()) {
    const ResponseBody& respBody = m->responsebody();
    if (respBody.has_subscriptionevent()) {
      const SubscriptionEventResponse& eventResp =
        respBody.subscriptionevent(); 
      // dispatch the event
      TopicSubscriber ts(m->topic(), m->subscriberid());
      handleSubscriptionEvent(ts, eventResp.event());
      return;
    }
  }
  
  HedwigClientChannelHandler::messageReceived(channel, m);
}

void SubscriberClientChannelHandler::doClose() {
  // clean the handler status
  closeHandler();

  if (channel.get()) {
    // need to ensure the channel is removed from allchannels list
    // since it will be killed
    channelManager->removeChannel(channel);
    LOG4CXX_INFO(logger, "remove subscription channel " << channel.get() << ".");
  }
}

SubscriberImpl::SubscriberImpl(const DuplexChannelManagerPtr& channelManager)
  : channelManager(channelManager) {
}

SubscriberImpl::~SubscriberImpl() {
  LOG4CXX_DEBUG(logger, "deleting subscriber" << this);
}

void SubscriberImpl::subscribe(const std::string& topic, const std::string& subscriberId, const SubscribeRequest::CreateOrAttach mode) {
  SubscriptionOptions options;
  options.set_createorattach(mode);
  subscribe(topic, subscriberId, options);
}

void SubscriberImpl::subscribe(const std::string& topic, const std::string& subscriberId, const SubscriptionOptions& options) {
  SyncOperationCallback* cb = new SyncOperationCallback(
    channelManager->getConfiguration().getInt(Configuration::SYNC_REQUEST_TIMEOUT,
                                              DEFAULT_SYNC_REQUEST_TIMEOUT));
  OperationCallbackPtr callback(cb);
  asyncSubscribe(topic, subscriberId, options, callback);
  cb->wait();
  
  cb->throwExceptionIfNeeded();
}

void SubscriberImpl::asyncSubscribe(const std::string& topic, const std::string& subscriberId, const SubscribeRequest::CreateOrAttach mode, const OperationCallbackPtr& callback) {
  SubscriptionOptions options;
  options.set_createorattach(mode);
  asyncSubscribe(topic, subscriberId, options, callback);
}

void SubscriberImpl::asyncSubscribe(const std::string& topic, const std::string& subscriberId, const SubscriptionOptions& options, const OperationCallbackPtr& callback) {
  SubscriptionOptions options2 = options;

  if (!options2.has_messagebound()) {
    int messageBound = channelManager->getConfiguration()
                       .getInt(Configuration::SUBSCRIPTION_MESSAGE_BOUND,
                               DEFAULT_SUBSCRIPTION_MESSAGE_BOUND);
    options2.set_messagebound(messageBound);
  }

  ResponseCallbackPtr respCallback(new ResponseCallbackAdaptor(callback));
  PubSubDataPtr data = PubSubData::forSubscribeRequest(channelManager->nextTxnId(),
                                                       subscriberId, topic,
                                                       respCallback, options2);
  channelManager->submitOp(data);
}

void SubscriberImpl::unsubscribe(const std::string& topic, const std::string& subscriberId) {
  SyncOperationCallback* cb = new SyncOperationCallback(
    channelManager->getConfiguration().getInt(Configuration::SYNC_REQUEST_TIMEOUT,
                                              DEFAULT_SYNC_REQUEST_TIMEOUT));
  OperationCallbackPtr callback(cb);
  asyncUnsubscribe(topic, subscriberId, callback);
  cb->wait();
  
  cb->throwExceptionIfNeeded();
}

void SubscriberImpl::asyncUnsubscribe(const std::string& topic, const std::string& subscriberId, const OperationCallbackPtr& callback) {
  OperationCallbackPtr closeCb(new CloseSubscriptionForUnsubscribeCallback(channelManager, topic,
                                                                           subscriberId, callback));
  asyncCloseSubscription(topic, subscriberId, closeCb);
}

void SubscriberImpl::consume(const std::string& topic, const std::string& subscriberId,
                             const MessageSeqId& messageSeqId) {
  TopicSubscriber t(topic, subscriberId);

  // Get the subscriber channel handler
  SubscriberClientChannelHandlerPtr handler =
    channelManager->getSubscriptionChannelHandler(t);

  if (handler.get() == 0) {
    LOG4CXX_ERROR(logger, "Cannot consume. No subscription channel handler found for topic ("
                          << topic << ") subscriberId(" << subscriberId << ").");
    return;
  }

  handler->consume(t, messageSeqId);
}

void SubscriberImpl::startDeliveryWithFilter(const std::string& topic,
                                             const std::string& subscriberId,
                                             const MessageHandlerCallbackPtr& callback,
                                             const ClientMessageFilterPtr& filter) {
  if (0 == filter.get()) {
    throw NullMessageFilterException();
  }
  if (0 == callback.get()) {
    throw NullMessageHandlerException();
  }

  TopicSubscriber t(topic, subscriberId);

  // Get the subscriber channel handler
  SubscriberClientChannelHandlerPtr handler =
    channelManager->getSubscriptionChannelHandler(t);

  if (handler.get() == 0) {
    LOG4CXX_ERROR(logger, "Trying to start deliver on a non existant handler topic = "
                          << topic << ", subscriber = " << subscriberId);
    throw NotSubscribedException();
  }

  handler->startDelivery(t, callback, filter);
}

void SubscriberImpl::startDelivery(const std::string& topic, const std::string& subscriberId,
                                   const MessageHandlerCallbackPtr& callback) {
  TopicSubscriber t(topic, subscriberId);

  // Get the subscriber channel handler
  SubscriberClientChannelHandlerPtr handler =
    channelManager->getSubscriptionChannelHandler(t);

  if (handler.get() == 0) {
    LOG4CXX_ERROR(logger, "Trying to start deliver on a non existant handler topic = "
                          << topic << ", subscriber = " << subscriberId);
    throw NotSubscribedException();
  }
  handler->startDelivery(t, callback, ClientMessageFilterPtr());
}

void SubscriberImpl::stopDelivery(const std::string& topic, const std::string& subscriberId) {
  TopicSubscriber t(topic, subscriberId);

  // Get the subscriber channel handler
  SubscriberClientChannelHandlerPtr handler =
    channelManager->getSubscriptionChannelHandler(t);

  if (handler.get() == 0) {
    LOG4CXX_ERROR(logger, "Trying to stop deliver on a non existant handler topic = "
                          << topic << ", subscriber = " << subscriberId);
    throw NotSubscribedException();
  }
  handler->stopDelivery(t);
}

bool SubscriberImpl::hasSubscription(const std::string& topic, const std::string& subscriberId) {
  TopicSubscriber ts(topic, subscriberId);
  // Get the subscriber channel handler
  SubscriberClientChannelHandlerPtr handler =
    channelManager->getSubscriptionChannelHandler(ts);
  if (!handler.get()) {
    return false;
  }
  return handler->hasSubscription(ts);
}

void SubscriberImpl::closeSubscription(const std::string& topic, const std::string& subscriberId) {
  SyncOperationCallback* cb = new SyncOperationCallback(
    channelManager->getConfiguration().getInt(Configuration::SYNC_REQUEST_TIMEOUT,
                                              DEFAULT_SYNC_REQUEST_TIMEOUT));
  OperationCallbackPtr callback(cb);
  asyncCloseSubscription(topic, subscriberId, callback);
  cb->wait();

  cb->throwExceptionIfNeeded();
}

void SubscriberImpl::asyncCloseSubscription(const std::string& topic,
                                            const std::string& subscriberId,
                                            const OperationCallbackPtr& callback) {
  LOG4CXX_INFO(logger, "closeSubscription (" << topic << ",  " << subscriberId << ")");

  TopicSubscriber t(topic, subscriberId);
  channelManager->asyncCloseSubscription(t, callback);
}

void SubscriberImpl::addSubscriptionListener(SubscriptionListenerPtr& listener) {
  channelManager->getEventEmitter().addSubscriptionListener(listener);
}

void SubscriberImpl::removeSubscriptionListener(SubscriptionListenerPtr& listener) {
  channelManager->getEventEmitter().removeSubscriptionListener(listener);
}

//
// Unsubscribe Response Handler
//
UnsubscribeResponseHandler::UnsubscribeResponseHandler(const DuplexChannelManagerPtr& channelManager)
  : ResponseHandler(channelManager) {}

void UnsubscribeResponseHandler::handleResponse(const PubSubResponsePtr& m,
                                                const PubSubDataPtr& txn,
                                                const DuplexChannelPtr& channel) {
  switch (m->statuscode()) {
  case SUCCESS:
    if (m->has_responsebody()) {
      txn->getCallback()->operationComplete(m->responsebody());
    } else {
      txn->getCallback()->operationComplete(ResponseBody());
    }
    break;
  case SERVICE_DOWN:
    LOG4CXX_ERROR(logger, "Server responsed with SERVICE_DOWN for " << txn->getTxnId());
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
// CloseSubscription Response Handler
//
CloseSubscriptionResponseHandler::CloseSubscriptionResponseHandler(
  const DuplexChannelManagerPtr& channelManager) : ResponseHandler(channelManager) {}

void CloseSubscriptionResponseHandler::handleResponse(
  const PubSubResponsePtr& m, const PubSubDataPtr& txn,
  const DuplexChannelPtr& channel) {
  switch (m->statuscode()) {
  case SUCCESS:
    if (m->has_responsebody()) {
      txn->getCallback()->operationComplete(m->responsebody());
    } else {
      txn->getCallback()->operationComplete(ResponseBody());
    }
    break;
  case SERVICE_DOWN:
    LOG4CXX_ERROR(logger, "Server responsed with SERVICE_DOWN for " << txn->getTxnId());
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

std::ostream& Hedwig::operator<<(std::ostream& os, const ActiveSubscriber& subscriber) {
  os << "ActiveSubscriber(" << &subscriber << ", topic:" << subscriber.getTopic()
     << ", subscriber:" << subscriber.getSubscriberId() << ")";
  return os;
}
