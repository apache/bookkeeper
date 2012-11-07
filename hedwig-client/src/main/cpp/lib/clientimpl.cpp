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

#include "clientimpl.h"
#include "channel.h"
#include "publisherimpl.h"
#include "subscriberimpl.h"
#include "simplesubscriberimpl.h"
#include "multiplexsubscriberimpl.h"
#include <log4cxx/logger.h>

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("hedwig."__FILE__));

using namespace Hedwig;

const int DEFAULT_MESSAGE_FORCE_CONSUME_RETRY_WAIT_TIME = 5000;
const std::string DEFAULT_SERVER_DEFAULT_VAL = "";
const bool DEFAULT_SSL_ENABLED = false;

void SyncOperationCallback::wait() {
  boost::unique_lock<boost::mutex> lock(mut);
  while(response==PENDING) {
    if (cond.timed_wait(lock, boost::posix_time::milliseconds(timeout)) == false) {
      LOG4CXX_ERROR(logger, "Timeout waiting for operation to complete " << this);

      response = TIMEOUT;
    }
  }
}

void SyncOperationCallback::operationComplete() {
  if (response == TIMEOUT) {
    LOG4CXX_ERROR(logger, "operationCompleted successfully after timeout " << this);
    return;
  }

  {
    boost::lock_guard<boost::mutex> lock(mut);
    response = SUCCESS;
  }
  cond.notify_all();
}

void SyncOperationCallback::operationFailed(const std::exception& exception) {
  if (response == TIMEOUT) {
    LOG4CXX_ERROR(logger, "operationCompleted unsuccessfully after timeout " << this);
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

void SyncOperationCallback::throwExceptionIfNeeded() {
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

ResponseHandler::ResponseHandler(const DuplexChannelManagerPtr& channelManager)
  : channelManager(channelManager) {
}

void ResponseHandler::redirectRequest(const PubSubResponsePtr& response,
                                      const PubSubDataPtr& data,
                                      const DuplexChannelPtr& channel) {
  HostAddress oldhost = channel->getHostAddress();
  data->addTriedServer(oldhost);

  HostAddress h;
  bool redirectToDefaultHost = true;
  try {
    if (response->has_statusmsg()) {
      try {
        h = HostAddress::fromString(response->statusmsg());
        redirectToDefaultHost = false;
      } catch (std::exception& e) {
        h = channelManager->getDefaultHost();
      }
    } else {
      h = channelManager->getDefaultHost();
    }
  } catch (std::exception& e) {
    LOG4CXX_ERROR(logger, "Failed to retrieve redirected host of request " << *data
                          << " : " << e.what());
    data->getCallback()->operationFailed(InvalidRedirectException());
    return;
  }
  if (data->hasTriedServer(h)) {
    LOG4CXX_ERROR(logger, "We've been told to try request [" << data->getTxnId() << "] with [" 
		                      << h.getAddressString()<< "] by " << oldhost.getAddressString() 
		                      << " but we've already tried that. Failing operation");
    data->getCallback()->operationFailed(InvalidRedirectException());
    return;
  }
  LOG4CXX_INFO(logger, "We've been told  [" << data->getTopic() << "] is on [" << h.getAddressString() 
		                   << "] by [" << oldhost.getAddressString() << "]. Redirecting request "
                       << data->getTxnId());
  data->setShouldClaim(true);

  // submit the request again to the target host
  if (redirectToDefaultHost) {
    channelManager->submitOpToDefaultServer(data);
  } else {
    channelManager->redirectOpToHost(data, h);
  }
}

HedwigClientChannelHandler::HedwigClientChannelHandler(const DuplexChannelManagerPtr& channelManager,
                                                       ResponseHandlerMap& handlers)
  : channelManager(channelManager), handlers(handlers), closed(false), disconnected(false) {
}

void HedwigClientChannelHandler::messageReceived(const DuplexChannelPtr& channel, const PubSubResponsePtr& m) {
  LOG4CXX_DEBUG(logger, "Message received txnid(" << m->txnid() << ") status(" 
		<< m->statuscode() << ")");
  if (m->has_message()) {
    LOG4CXX_ERROR(logger, "Subscription response, ignore for now");
    return;
  }
  
  PubSubDataPtr data = channel->retrieveTransaction(m->txnid()); 
  /* you now have ownership of data, don't leave this funciton without deleting it or 
     palming it off to someone else */

  if (data.get() == 0) {
    LOG4CXX_ERROR(logger, "No pub/sub request for txnid(" << m->txnid() << ").");
    return;
  }

  // Store the topic2Host mapping if this wasn't a server redirect.
  // TODO: add specific response for failure of getting topic ownership
  //       to distinguish SERVICE_DOWN to failure of getting topic ownership
  if (m->statuscode() != NOT_RESPONSIBLE_FOR_TOPIC) {
    const HostAddress& host = channel->getHostAddress();
    channelManager->setHostForTopic(data->getTopic(), host);
  }

  const ResponseHandlerPtr& respHandler = handlers[data->getType()];
  if (respHandler.get()) {
    respHandler->handleResponse(m, data, channel);
  } else {
    LOG4CXX_ERROR(logger, "Unimplemented request type " << data->getType() << " : "
                          << *data);
    data->getCallback()->operationFailed(UnknownRequestException());
  }
}

void HedwigClientChannelHandler::channelConnected(const DuplexChannelPtr& channel) {
  // do nothing 
}

void HedwigClientChannelHandler::channelDisconnected(const DuplexChannelPtr& channel,
                                                     const std::exception& e) {
  if (channelManager->isClosed()) {
    return;
  }

  // If this channel was closed explicitly by the client code,
  // we do not need to do any of this logic. This could happen
  // for redundant Publish channels created or redirected subscribe
  // channels that are not used anymore or when we shutdown the
  // client and manually close all of the open channels.
  // Also don't do any of the disconnect logic if the client has stopped.
  {
    boost::lock_guard<boost::shared_mutex> lock(close_lock);
    if (closed) {
      return;
    }
    if (disconnected) {
      return;
    }
    disconnected = true;
  }
  LOG4CXX_INFO(logger, "Channel " << channel.get() << " was disconnected.");
  // execute logic after channel disconnected
  onChannelDisconnected(channel);
}

void HedwigClientChannelHandler::onChannelDisconnected(const DuplexChannelPtr& channel) {
  // Clean up the channel from channel manager
  channelManager->nonSubscriptionChannelDied(channel);
}

void HedwigClientChannelHandler::exceptionOccurred(const DuplexChannelPtr& channel, const std::exception& e) {
  LOG4CXX_ERROR(logger, "Exception occurred" << e.what());
}

void HedwigClientChannelHandler::close() {
  {
    boost::lock_guard<boost::shared_mutex> lock(close_lock);
    if (closed) {
      return;
    }
    closed = true;
  }
  // do close handle logic here
  doClose();
}

void HedwigClientChannelHandler::doClose() {
  // do nothing for generic client channel handler
}

//
// Pub/Sub Request Write Callback
//
PubSubWriteCallback::PubSubWriteCallback(const DuplexChannelPtr& channel,
                                         const PubSubDataPtr& data)
  : channel(channel), data(data) {
}

void PubSubWriteCallback::operationComplete() {
  LOG4CXX_INFO(logger, "Successfully wrote pubsub request : " << *data << " to channel "
                       << channel.get());
}

void PubSubWriteCallback::operationFailed(const std::exception& exception) {
  LOG4CXX_ERROR(logger, "Error writing pubsub request (" << *data << ") : " << exception.what());

  // remove the transaction from channel if write failed
  channel->retrieveTransaction(data->getTxnId());
  data->getCallback()->operationFailed(exception);
}

//
// Default Server Connect Callback
//
DefaultServerConnectCallback::DefaultServerConnectCallback(const DuplexChannelManagerPtr& channelManager,
                                                           const DuplexChannelPtr& channel,
                                                           const PubSubDataPtr& data)
  : channelManager(channelManager), channel(channel), data(data) {
}

void DefaultServerConnectCallback::operationComplete() {
  LOG4CXX_DEBUG(logger, "Channel " << channel.get() << " is connected to host "
                        << channel->getHostAddress() << ".");
  // After connected, we got the right ip for the target host
  // so we could submit the request right now
  channelManager->submitOpThruChannel(data, channel);
}

void DefaultServerConnectCallback::operationFailed(const std::exception& exception) {
  LOG4CXX_ERROR(logger, "Channel " << channel.get() << " failed to connect to host "
                        << channel->getHostAddress() << " : " << exception.what());
  data->getCallback()->operationFailed(exception);
}

//
// Subscription Event Emitter
//
SubscriptionEventEmitter::SubscriptionEventEmitter() {}

void SubscriptionEventEmitter::addSubscriptionListener(
  SubscriptionListenerPtr& listener) {
  boost::lock_guard<boost::shared_mutex> lock(listeners_lock);
  listeners.insert(listener);
}

void SubscriptionEventEmitter::removeSubscriptionListener(
  SubscriptionListenerPtr& listener) {
  boost::lock_guard<boost::shared_mutex> lock(listeners_lock);
  listeners.erase(listener);
}

void SubscriptionEventEmitter::emitSubscriptionEvent(
  const std::string& topic, const std::string& subscriberId,
  const SubscriptionEvent event) {
  boost::shared_lock<boost::shared_mutex> lock(listeners_lock);
  if (0 == listeners.size()) {
    return;
  }
  for (SubscriptionListenerSet::iterator iter = listeners.begin();
       iter != listeners.end(); ++iter) {
    (*iter)->processEvent(topic, subscriberId, event);
  }
}

//
// Channel Manager Used to manage all established channels
//

DuplexChannelManagerPtr DuplexChannelManager::create(const Configuration& conf) {
  DuplexChannelManager * managerPtr;
  if (conf.getBool(Configuration::SUBSCRIPTION_CHANNEL_SHARING_ENABLED, false)) {
    managerPtr = new MultiplexDuplexChannelManager(conf);
  } else {
    managerPtr = new SimpleDuplexChannelManager(conf);
  }
  DuplexChannelManagerPtr manager(managerPtr);
  LOG4CXX_DEBUG(logger, "Created DuplexChannelManager " << manager.get());
  return manager;
}

DuplexChannelManager::DuplexChannelManager(const Configuration& conf)
  : dispatcher(new EventDispatcher(conf)), conf(conf), closed(false), counterobj(),
    defaultHostAddress(conf.get(Configuration::DEFAULT_SERVER,
                                DEFAULT_SERVER_DEFAULT_VAL)) {
  sslEnabled = conf.getBool(Configuration::SSL_ENABLED, DEFAULT_SSL_ENABLED); 
  if (sslEnabled) {
    sslCtxFactory = SSLContextFactoryPtr(new SSLContextFactory(conf));
  }
  LOG4CXX_DEBUG(logger, "Created DuplexChannelManager " << this << " with default server "
                        << defaultHostAddress);
}

DuplexChannelManager::~DuplexChannelManager() {
  LOG4CXX_DEBUG(logger, "Destroyed DuplexChannelManager " << this);
}

void DuplexChannelManager::submitTo(const PubSubDataPtr& op, const DuplexChannelPtr& channel) {
  if (channel.get()) {
    channel->storeTransaction(op);
    OperationCallbackPtr writecb(new PubSubWriteCallback(channel, op));
    LOG4CXX_DEBUG(logger, "Submit pub/sub request " << *op << " thru channel " << channel.get());
    channel->writeRequest(op->getRequest(), writecb);
  } else {
    submitOpToDefaultServer(op);
  }
}
    
// Submit a pub/sub request
void DuplexChannelManager::submitOp(const PubSubDataPtr& op) {
  DuplexChannelPtr channel;
  switch (op->getType()) {
  case PUBLISH:
  case UNSUBSCRIBE:
    try {
      channel = getNonSubscriptionChannel(op->getTopic());  
    } catch (std::exception& e) {
      LOG4CXX_ERROR(logger, "Failed to submit request " << *op << " : " << e.what());
      op->getCallback()->operationFailed(e);
      return;
    }
    break;
  default:
    TopicSubscriber ts(op->getTopic(), op->getSubscriberId());
    channel = getSubscriptionChannel(ts, op->isResubscribeRequest());
    break;
  }
  // write the pub/sub request
  submitTo(op, channel);
}

// Submit a pub/sub request to target host
void DuplexChannelManager::redirectOpToHost(const PubSubDataPtr& op, const HostAddress& addr) {
  DuplexChannelPtr channel;
  switch (op->getType()) {
  case PUBLISH:
  case UNSUBSCRIBE:
    // check whether there is a channel existed for non-subscription requests
    channel = getNonSubscriptionChannel(addr);
    if (!channel.get()) {
      channel = createNonSubscriptionChannel(addr);
      channel = storeNonSubscriptionChannel(channel, true);
    }
    break;
  default:
    channel = getSubscriptionChannel(addr);
    if (!channel.get()) {
      channel = createSubscriptionChannel(addr);
      channel = storeSubscriptionChannel(channel, true);
    }
    break;
  }
  // write the pub/sub request
  submitTo(op, channel);
}

// Submit a pub/sub request to established request
void DuplexChannelManager::submitOpThruChannel(const PubSubDataPtr& op,
                                               const DuplexChannelPtr& ch) {
  DuplexChannelPtr channel;
  switch (op->getType()) {
  case PUBLISH:
  case UNSUBSCRIBE:
    channel = storeNonSubscriptionChannel(ch, false);
    break;
  default:
    channel = storeSubscriptionChannel(ch, false);
    break;
  }
  // write the pub/sub request
  submitTo(op, channel);
}

// Submit a pub/sub request to default server
void DuplexChannelManager::submitOpToDefaultServer(const PubSubDataPtr& op) {
  DuplexChannelPtr channel;
  try {
    switch (op->getType()) {
    case PUBLISH:
    case UNSUBSCRIBE:
      channel = createNonSubscriptionChannel(getDefaultHost());
      break;
    default:
      channel = createSubscriptionChannel(getDefaultHost());
      break;
    }
  } catch (std::exception& e) {
    LOG4CXX_ERROR(logger, "Failed to create channel to default host " << defaultHostAddress
                          << " for request " << op << " : " << e.what());
    op->getCallback()->operationFailed(e);
    return;
  }
  OperationCallbackPtr connectCallback(new DefaultServerConnectCallback(shared_from_this(),
                                                                        channel, op));
  // connect to default server. usually default server is a VIP, we only got the real
  // IP address after connected. so before connected, we don't know the real target host.
  // we only submit the request after channel is connected (ip address would be updated).
  channel->connect(connectCallback);
}

DuplexChannelPtr DuplexChannelManager::getNonSubscriptionChannel(const std::string& topic) {
  HostAddress addr;
  {
    boost::shared_lock<boost::shared_mutex> lock(topic2host_lock);
    addr = topic2host[topic];
  }
  if (addr.isNullHost()) {
    return DuplexChannelPtr();
  } else {
    // we had known which hub server owned the topic
    DuplexChannelPtr ch = getNonSubscriptionChannel(addr);
    if (ch.get()) {
      return ch;
    }
    ch = createNonSubscriptionChannel(addr);
    return storeNonSubscriptionChannel(ch, true);
  }
}

DuplexChannelPtr DuplexChannelManager::getNonSubscriptionChannel(const HostAddress& addr) {
  boost::shared_lock<boost::shared_mutex> lock(host2channel_lock);
  return host2channel[addr];
}

DuplexChannelPtr DuplexChannelManager::createNonSubscriptionChannel(const HostAddress& addr) {
  // Create a non-subscription channel handler
  ChannelHandlerPtr handler(new HedwigClientChannelHandler(shared_from_this(),
                                                           nonSubscriptionHandlers));
  // Create a non subscription channel
  return createChannel(dispatcher->getService(), addr, handler);
}

DuplexChannelPtr DuplexChannelManager::storeNonSubscriptionChannel(const DuplexChannelPtr& ch,
                                                                   bool doConnect) {
  const HostAddress& host = ch->getHostAddress();

  bool useOldCh;
  DuplexChannelPtr oldCh;
  {
    boost::lock_guard<boost::shared_mutex> lock(host2channel_lock);

    oldCh = host2channel[host];
    if (!oldCh.get()) {
      host2channel[host] = ch;
      useOldCh = false;
    } else {
      // If we've reached here, that means we already have a Channel
      // mapping for the given host. This should ideally not happen
      // and it means we are creating another Channel to a server host
      // to publish on when we could have used an existing one. This could
      // happen due to a race condition if initially multiple concurrent
      // threads are publishing on the same topic and no Channel exists
      // currently to the server. We are not synchronizing this initial
      // creation of Channels to a given host for performance.
      // Another possible way to have redundant Channels created is if
      // a new topic is being published to, we connect to the default
      // server host which should be a VIP that redirects to a "real"
      // server host. Since we don't know beforehand what is the full
      // set of server hosts, we could be redirected to a server that
      // we already have a channel connection to from a prior existing
      // topic. Close these redundant channels as they won't be used.
      useOldCh = true;
    }
  } 
  if (useOldCh) {
    LOG4CXX_DEBUG(logger, "Channel " << oldCh.get() << " to host " << host
                          << " already exists so close channel " << ch.get() << ".");
    ch->close();
    return oldCh;
  } else {
    if (doConnect) {
      ch->connect();
    }
    LOG4CXX_DEBUG(logger, "Storing channel " << ch.get() << " for host " << host << ".");
    return ch;
  }
}

DuplexChannelPtr DuplexChannelManager::createChannel(IOServicePtr& service,
                                                     const HostAddress& addr,
                                                     const ChannelHandlerPtr& handler) {
  DuplexChannelPtr channel;
  if (sslEnabled) {
    boost_ssl_context_ptr sslCtx = sslCtxFactory->createSSLContext(service->getService());
    channel = DuplexChannelPtr(new AsioSSLDuplexChannel(service, sslCtx, addr, handler));
  } else {
    channel = DuplexChannelPtr(new AsioDuplexChannel(service, addr, handler));
  }

  boost::lock_guard<boost::shared_mutex> lock(allchannels_lock);
  if (closed) {
    channel->close();
    throw ShuttingDownException();
  }
  allchannels.insert(channel);
  LOG4CXX_DEBUG(logger, "Created a channel to " << addr << ", all channels : " << allchannels.size());

  return channel;
}

long DuplexChannelManager::nextTxnId() {
  return counterobj.next();
}

void DuplexChannelManager::setHostForTopic(const std::string& topic, const HostAddress& host) {
  boost::lock_guard<boost::shared_mutex> h2clock(host2topics_lock);
  boost::lock_guard<boost::shared_mutex> t2hlock(topic2host_lock);
  topic2host[topic] = host;
  TopicSetPtr ts = host2topics[host];
  if (!ts.get()) {
    ts = TopicSetPtr(new TopicSet());
    host2topics[host] = ts;
  }
  ts->insert(topic);
  LOG4CXX_DEBUG(logger, "Set ownership of topic " << topic << " to " << host << ".");
}

void DuplexChannelManager::clearAllTopicsForHost(const HostAddress& addr) {
  // remove topic mapping
  boost::lock_guard<boost::shared_mutex> h2tlock(host2topics_lock);
  boost::lock_guard<boost::shared_mutex> t2hlock(topic2host_lock);
  Host2TopicsMap::iterator iter = host2topics.find(addr);
  if (iter != host2topics.end()) {
    for (TopicSet::iterator tsIter = iter->second->begin();
         tsIter != iter->second->end(); ++tsIter) {
      topic2host.erase(*tsIter);
    }
    host2topics.erase(iter);
  }
}

void DuplexChannelManager::clearHostForTopic(const std::string& topic,
                                             const HostAddress& addr) {
  // remove topic mapping
  boost::lock_guard<boost::shared_mutex> h2tlock(host2topics_lock);
  boost::lock_guard<boost::shared_mutex> t2hlock(topic2host_lock);
  Host2TopicsMap::iterator iter = host2topics.find(addr);
  if (iter != host2topics.end()) {
    iter->second->erase(topic);
  }
  HostAddress existed = topic2host[topic];
  if (existed == addr) {
    topic2host.erase(topic);
  }
}

const HostAddress& DuplexChannelManager::getHostForTopic(const std::string& topic) {
  boost::shared_lock<boost::shared_mutex> t2hlock(topic2host_lock);
  return topic2host[topic];
}

/**
   A channel has just died. Remove it so we never give it to any other publisher or subscriber.
   
   This does not delete the channel. Some publishers or subscribers will still hold it and will be errored
   when they try to do anything with it. 
*/
void DuplexChannelManager::nonSubscriptionChannelDied(const DuplexChannelPtr& channel) {
  // get host
  HostAddress addr = channel->getHostAddress();

  // Clear the topic owner ship when a nonsubscription channel disconnected
  clearAllTopicsForHost(addr);

  // remove channel mapping
  {
    boost::lock_guard<boost::shared_mutex> h2clock(host2channel_lock);
    host2channel.erase(addr);
  }
  removeChannel(channel);
}

void DuplexChannelManager::removeChannel(const DuplexChannelPtr& channel) {
  {
    boost::lock_guard<boost::shared_mutex> aclock(allchannels_lock);
    allchannels.erase(channel); // channel should be deleted here
  }
  channel->close();
}

void DuplexChannelManager::start() {
  // add non-subscription response handlers
  nonSubscriptionHandlers[PUBLISH] =
    ResponseHandlerPtr(new PublishResponseHandler(shared_from_this()));
  nonSubscriptionHandlers[UNSUBSCRIBE] =
    ResponseHandlerPtr(new UnsubscribeResponseHandler(shared_from_this()));

  // start the dispatcher
  dispatcher->start();
}

bool DuplexChannelManager::isClosed() {
  boost::shared_lock<boost::shared_mutex> lock(allchannels_lock);
  return closed;
}

void DuplexChannelManager::close() {
  // stop the dispatcher
  dispatcher->stop();
  {
    boost::lock_guard<boost::shared_mutex> lock(allchannels_lock);
    
    closed = true;
    for (ChannelMap::iterator iter = allchannels.begin(); iter != allchannels.end(); ++iter ) {
      (*iter)->close();
    }  
    allchannels.clear();
  }

  // Unregistered response handlers
  nonSubscriptionHandlers.clear();
  /* destruction of the maps will clean up any items they hold */
}

ClientImplPtr ClientImpl::Create(const Configuration& conf) {
  ClientImplPtr impl(new ClientImpl(conf));
  LOG4CXX_DEBUG(logger, "Creating Clientimpl " << impl);
  impl->channelManager->start();
  return impl;
}

void ClientImpl::Destroy() {
  LOG4CXX_DEBUG(logger, "destroying Clientimpl " << this);

  // close the channel manager
  channelManager->close();

  if (subscriber != NULL) {
    delete subscriber;
    subscriber = NULL;
  }
  if (publisher != NULL) {
    delete publisher;
    publisher = NULL;
  }
}

ClientImpl::ClientImpl(const Configuration& conf) 
  : conf(conf), publisher(NULL), subscriber(NULL)
{
  channelManager = DuplexChannelManager::create(conf);
}

Subscriber& ClientImpl::getSubscriber() {
  return getSubscriberImpl();
}

Publisher& ClientImpl::getPublisher() {
  return getPublisherImpl();
}
    
SubscriberImpl& ClientImpl::getSubscriberImpl() {
  if (subscriber == NULL) {
    boost::lock_guard<boost::mutex> lock(subscribercreate_lock);
    if (subscriber == NULL) {
      subscriber = new SubscriberImpl(channelManager);
    }
  }
  return *subscriber;
}

PublisherImpl& ClientImpl::getPublisherImpl() {
  if (publisher == NULL) { 
    boost::lock_guard<boost::mutex> lock(publishercreate_lock);
    if (publisher == NULL) {
      publisher = new PublisherImpl(channelManager);
    }
  }
  return *publisher;
}

ClientImpl::~ClientImpl() {
  LOG4CXX_DEBUG(logger, "deleting Clientimpl " << this);
}
