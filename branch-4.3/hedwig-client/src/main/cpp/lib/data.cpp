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

#include <hedwig/protocol.h>
#include "data.h"

#include <log4cxx/logger.h>
#include <iostream>
#include <boost/thread/locks.hpp>

#define stringify( name ) #name

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("hedwig."__FILE__));

using namespace Hedwig;

const char* OPERATION_TYPE_NAMES[] = {
  stringify( PUBLISH ),
  stringify( SUBSCRIBE ),
  stringify( CONSUME ),
  stringify( UNSUBSCRIBE ),
  stringify( START_DELIVERY ),
  stringify( STOP_DELIVERY ),
  stringify( CLOSESUBSCRIPTION )
};

PubSubDataPtr PubSubData::forPublishRequest(long txnid, const std::string& topic, const Message& body,
                                            const ResponseCallbackPtr& callback) {
  PubSubDataPtr ptr(new PubSubData());
  ptr->type = PUBLISH;
  ptr->txnid = txnid;
  ptr->topic = topic;
  ptr->body.CopyFrom(body);
  ptr->callback = callback;
  return ptr;
}

PubSubDataPtr PubSubData::forSubscribeRequest(long txnid, const std::string& subscriberid, const std::string& topic,
                                              const ResponseCallbackPtr& callback, const SubscriptionOptions& options) {
  PubSubDataPtr ptr(new PubSubData());
  ptr->type = SUBSCRIBE;
  ptr->txnid = txnid;
  ptr->subscriberid = subscriberid;
  ptr->topic = topic;
  ptr->callback = callback;
  ptr->options = options;
  return ptr;  
}

PubSubDataPtr PubSubData::forUnsubscribeRequest(long txnid, const std::string& subscriberid, const std::string& topic,
                                                const ResponseCallbackPtr& callback) {
  PubSubDataPtr ptr(new PubSubData());
  ptr->type = UNSUBSCRIBE;
  ptr->txnid = txnid;
  ptr->subscriberid = subscriberid;
  ptr->topic = topic;
  ptr->callback = callback;
  return ptr;  
}

PubSubDataPtr PubSubData::forCloseSubscriptionRequest(
  long txnid, const std::string& subscriberid, const std::string& topic,
  const ResponseCallbackPtr& callback) {
  PubSubDataPtr ptr(new PubSubData());
  ptr->type = CLOSESUBSCRIPTION;
  ptr->txnid = txnid;
  ptr->subscriberid = subscriberid;
  ptr->topic = topic;
  ptr->callback = callback;
  return ptr;  
}

PubSubDataPtr PubSubData::forConsumeRequest(long txnid, const std::string& subscriberid, const std::string& topic, const MessageSeqId msgid) {
  PubSubDataPtr ptr(new PubSubData());
  ptr->type = CONSUME;
  ptr->txnid = txnid;
  ptr->subscriberid = subscriberid;
  ptr->topic = topic;
  ptr->msgid = msgid;
  return ptr;  
}

PubSubData::PubSubData() : shouldClaim(false), messageBound(0) {  
}

PubSubData::~PubSubData() {
}

OperationType PubSubData::getType() const {
  return type;
}

long PubSubData::getTxnId() const {
  return txnid;
}

const std::string& PubSubData::getTopic() const {
  return topic;
}

const Message& PubSubData::getBody() const {
  return body;
}

const MessageSeqId PubSubData::getMessageSeqId() const {
  return msgid;
}

void PubSubData::setPreferencesForSubRequest(SubscribeRequest * subreq,
                                             const SubscriptionOptions &options) {
  Hedwig::SubscriptionPreferences* preferences = subreq->mutable_preferences();
  if (options.messagebound() > 0) {
    preferences->set_messagebound(options.messagebound());
  }
  if (options.has_messagefilter()) {
    preferences->set_messagefilter(options.messagefilter());
  }
  if (options.has_options()) {
    preferences->mutable_options()->CopyFrom(options.options());
  }
  if (options.has_messagewindowsize()) {
    preferences->set_messagewindowsize(options.messagewindowsize());
  }
}

const PubSubRequestPtr PubSubData::getRequest() {
  PubSubRequestPtr request(new Hedwig::PubSubRequest());
  request->set_protocolversion(Hedwig::VERSION_ONE);
  request->set_type(type);
  request->set_txnid(txnid);
  if (shouldClaim) {
    request->set_shouldclaim(shouldClaim);
  }
  request->set_topic(topic);
    
  if (type == PUBLISH) {
    LOG4CXX_DEBUG(logger, "Creating publish request");

    Hedwig::PublishRequest* pubreq = request->mutable_publishrequest();
    Hedwig::Message* msg = pubreq->mutable_msg();
    msg->CopyFrom(body);
  } else if (type == SUBSCRIBE) {
    LOG4CXX_DEBUG(logger, "Creating subscribe request");

    Hedwig::SubscribeRequest* subreq = request->mutable_subscriberequest();
    subreq->set_subscriberid(subscriberid);
    subreq->set_createorattach(options.createorattach());
    subreq->set_forceattach(options.forceattach());
    setPreferencesForSubRequest(subreq, options);
  } else if (type == CONSUME) {
    LOG4CXX_DEBUG(logger, "Creating consume request");

    Hedwig::ConsumeRequest* conreq = request->mutable_consumerequest();
    conreq->set_subscriberid(subscriberid);
    conreq->mutable_msgid()->CopyFrom(msgid);
  } else if (type == UNSUBSCRIBE) {
    LOG4CXX_DEBUG(logger, "Creating unsubscribe request");
    
    Hedwig::UnsubscribeRequest* unsubreq = request->mutable_unsubscriberequest();
    unsubreq->set_subscriberid(subscriberid);    
  } else if (type == CLOSESUBSCRIPTION) {
    LOG4CXX_DEBUG(logger, "Creating closeSubscription request");
    
    Hedwig::CloseSubscriptionRequest* closesubreq = request->mutable_closesubscriptionrequest();
    closesubreq->set_subscriberid(subscriberid);    
  } else {
    LOG4CXX_ERROR(logger, "Tried to create a request message for the wrong type [" << type << "]");
    throw UnknownRequestException();
  }

  return request;
}

void PubSubData::setShouldClaim(bool shouldClaim) {
  this->shouldClaim = shouldClaim;
}

void PubSubData::addTriedServer(HostAddress& h) {
  triedservers.insert(h);
}

bool PubSubData::hasTriedServer(HostAddress& h) {
  return triedservers.count(h) > 0;
}

void PubSubData::clearTriedServers() {
  triedservers.clear();
}

ResponseCallbackPtr& PubSubData::getCallback() {
  return callback;
}

void PubSubData::setCallback(const ResponseCallbackPtr& callback) {
  this->callback = callback;
}

const std::string& PubSubData::getSubscriberId() const {
  return subscriberid;
}

const SubscriptionOptions& PubSubData::getSubscriptionOptions() const {
  return options;
}

void PubSubData::setOrigChannelForResubscribe(
  boost::shared_ptr<DuplexChannel>& channel) {
  this->origChannel = channel;
}

boost::shared_ptr<DuplexChannel>& PubSubData::getOrigChannelForResubscribe() {
  return this->origChannel;
}

bool PubSubData::isResubscribeRequest() {
  return 0 != this->origChannel.get();
}

ClientTxnCounter::ClientTxnCounter() : counter(0) 
{
}

ClientTxnCounter::~ClientTxnCounter() {
}

/**
Increment the transaction counter and return the new value.

@returns the next transaction id
*/
long ClientTxnCounter::next() {  // would be nice to remove lock from here, look more into it
  boost::lock_guard<boost::mutex> lock(mutex);

  long next= ++counter; 

  return next;
}

std::ostream& Hedwig::operator<<(std::ostream& os, const PubSubData& data) {
  OperationType type = data.getType();
  os << "[" << OPERATION_TYPE_NAMES[type] << " request (txn:" << data.getTxnId()
     << ") for (topic:" << data.getTopic();
  switch (type) {
  case SUBSCRIBE:
  case UNSUBSCRIBE:
  case CLOSESUBSCRIPTION:
    os << ", subscriber:" << data.getSubscriberId() << ")";
    break;
  case CONSUME:
    os << ", subscriber:" << data.getSubscriberId() << ", seq:"
    << data.getMessageSeqId().localcomponent() << ")";
    break;
  case PUBLISH:
  default:
    os << ")";
    break;
  }
  return os;
}
