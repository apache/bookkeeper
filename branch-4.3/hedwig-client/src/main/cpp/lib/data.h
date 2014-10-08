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

#ifndef DATA_H
#define DATA_H

#include <hedwig/protocol.h>
#include <hedwig/callback.h>

#include <pthread.h>
#include <iostream>

#ifdef USE_BOOST_TR1
#include <boost/tr1/unordered_set.hpp>
#else
#include <tr1/unordered_set>
#endif

#include "util.h"
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>

namespace Hedwig {
  /**
     Simple counter for transaction ids from the client
  */
  class ClientTxnCounter {
  public:
    ClientTxnCounter();
    ~ClientTxnCounter();
    long next();
    
  private:
    long counter;
    boost::mutex mutex;
  };

  typedef Callback<ResponseBody> ResponseCallback;
  typedef std::tr1::shared_ptr<ResponseCallback> ResponseCallbackPtr;

  class PubSubData;
  typedef boost::shared_ptr<PubSubData> PubSubDataPtr;
  typedef boost::shared_ptr<PubSubRequest> PubSubRequestPtr;
  typedef boost::shared_ptr<PubSubResponse> PubSubResponsePtr;

  class DuplexChannel;

  /**
     Data structure to hold information about requests and build request messages.
     Used to store requests which may need to be resent to another server. 
   */
  class PubSubData {
  public:
    // to be used for publish
    static PubSubDataPtr forPublishRequest(long txnid, const std::string& topic, const Message& body,
                                           const ResponseCallbackPtr& callback);
    static PubSubDataPtr forSubscribeRequest(long txnid, const std::string& subscriberid, const std::string& topic,
                                             const ResponseCallbackPtr& callback, const SubscriptionOptions& options);
    static PubSubDataPtr forUnsubscribeRequest(long txnid, const std::string& subscriberid, const std::string& topic,
                                               const ResponseCallbackPtr& callback);
    static PubSubDataPtr forConsumeRequest(long txnid, const std::string& subscriberid, const std::string& topic, const MessageSeqId msgid);

    static PubSubDataPtr forCloseSubscriptionRequest(long txnid, const std::string& subscriberid,
                                                     const std::string& topic,
                                                     const ResponseCallbackPtr& callback);

    ~PubSubData();

    OperationType getType() const;
    long getTxnId() const;
    const std::string& getSubscriberId() const;
    const std::string& getTopic() const;
    const Message& getBody() const;
    const MessageSeqId getMessageSeqId() const;

    void setShouldClaim(bool shouldClaim);
    void setMessageBound(int messageBound);

    const PubSubRequestPtr getRequest();
    void setCallback(const ResponseCallbackPtr& callback);
    ResponseCallbackPtr& getCallback();
    const SubscriptionOptions& getSubscriptionOptions() const;

    void addTriedServer(HostAddress& h);
    bool hasTriedServer(HostAddress& h);
    void clearTriedServers();

    void setOrigChannelForResubscribe(boost::shared_ptr<DuplexChannel>& channel);
    bool isResubscribeRequest();
    boost::shared_ptr<DuplexChannel>& getOrigChannelForResubscribe();

    friend std::ostream& operator<<(std::ostream& os, const PubSubData& data);
  private:

    PubSubData();

    void setPreferencesForSubRequest(SubscribeRequest * subreq,
                                     const SubscriptionOptions &options);
    
    OperationType type;
    long txnid;
    std::string subscriberid;
    std::string topic;
    Message body;
    bool shouldClaim;
    int messageBound;
    ResponseCallbackPtr callback;
    SubscriptionOptions options;
    MessageSeqId msgid;
    std::tr1::unordered_set<HostAddress, HostAddressHash > triedservers;
    // record the origChannel for a resubscribe request
    boost::shared_ptr<DuplexChannel> origChannel;
  };

};
#endif
