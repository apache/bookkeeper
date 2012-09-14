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
#ifndef HEDWIG_CALLBACK_H
#define HEDWIG_CALLBACK_H

#include <string>
#include <hedwig/exceptions.h>
#include <hedwig/protocol.h>

#ifdef USE_BOOST_TR1
#include <boost/tr1/memory.hpp>
#else 
#include <tr1/memory>
#endif

namespace Hedwig {

  //
  // A Listener registered for a Subscriber instance to emit events
  // for those disable resubscribe subscriptions.
  //
  class SubscriptionListener {
  public:
    virtual void processEvent(const std::string &topic, const std::string &subscriberId,
                              const Hedwig::SubscriptionEvent event) = 0;
    virtual ~SubscriptionListener() {};
  };
  typedef std::tr1::shared_ptr<SubscriptionListener> SubscriptionListenerPtr;

  template<class R>
  class Callback {
  public:
    virtual void operationComplete(const R& result) = 0;
    virtual void operationFailed(const std::exception& exception) = 0;

    virtual ~Callback() {};
  };

  class OperationCallback {
  public:
    virtual void operationComplete() = 0;
    virtual void operationFailed(const std::exception& exception) = 0;
    
    virtual ~OperationCallback() {};
  };
  typedef std::tr1::shared_ptr<OperationCallback> OperationCallbackPtr;

  class MessageHandlerCallback {
  public:
    virtual void consume(const std::string& topic, const std::string& subscriberId, const Message& msg, OperationCallbackPtr& callback) = 0;
    
    virtual ~MessageHandlerCallback() {};
  };
  typedef std::tr1::shared_ptr<MessageHandlerCallback> MessageHandlerCallbackPtr;

  typedef std::tr1::shared_ptr<SubscriptionPreferences> SubscriptionPreferencesPtr;

  class ClientMessageFilter {
  public:
    virtual void setSubscriptionPreferences(const std::string& topic, const std::string& subscriberId,
                                            const SubscriptionPreferencesPtr& preferences) = 0;
    virtual bool testMessage(const Message& message) = 0;

    virtual ~ClientMessageFilter() {};
  };
  typedef std::tr1::shared_ptr<ClientMessageFilter> ClientMessageFilterPtr;
}

#endif
