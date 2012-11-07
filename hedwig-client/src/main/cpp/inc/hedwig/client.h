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
#ifndef HEDWIG_CLIENT_H
#define HEDWIG_CLIENT_H

#include <string>

#ifdef USE_BOOST_TR1
#include <boost/tr1/memory.hpp>
#else 
#include <tr1/memory>
#endif

#include <hedwig/subscribe.h>
#include <hedwig/publish.h>
#include <hedwig/exceptions.h>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>

namespace Hedwig {

  class ClientImpl;
  typedef boost::shared_ptr<ClientImpl> ClientImplPtr;

  class Configuration {
  public:
    static const std::string DEFAULT_SERVER;
    static const std::string MESSAGE_CONSUME_RETRY_WAIT_TIME;
    static const std::string SUBSCRIBER_CONSUME_RETRY_WAIT_TIME;
    static const std::string MAX_MESSAGE_QUEUE_SIZE;
    static const std::string RECONNECT_SUBSCRIBE_RETRY_WAIT_TIME;
    static const std::string SYNC_REQUEST_TIMEOUT;
    static const std::string SUBSCRIBER_AUTOCONSUME;
    static const std::string NUM_DISPATCH_THREADS;
    static const std::string SSL_ENABLED;
    static const std::string SSL_PEM_FILE;
    static const std::string SUBSCRIPTION_CHANNEL_SHARING_ENABLED;
    /**
     * The maximum number of messages the hub will queue for subscriptions
     * created using this configuration. The hub will always queue the most
     * recent messages. If there are enough publishes to the topic to hit
     * the bound, then the oldest messages are dropped from the queue.
     *
     * A bound of 0 disabled the bound completely.
     */
    static const std::string SUBSCRIPTION_MESSAGE_BOUND;

  public:
    Configuration() {};
    virtual int getInt(const std::string& key, int defaultVal) const = 0;
    virtual const std::string get(const std::string& key, const std::string& defaultVal) const = 0;
    virtual bool getBool(const std::string& key, bool defaultVal) const = 0;

    virtual ~Configuration() {}
  };

  /** 
      Main Hedwig client class. This class is used to acquire an instance of the Subscriber of Publisher.
  */
  class Client : private boost::noncopyable {
  public: 
    Client(const Configuration& conf);

    /**
       Retrieve the subscriber object
    */
    Subscriber& getSubscriber();

    /**
       Retrieve the publisher object
    */
    Publisher& getPublisher();

    ~Client();

  private:
    ClientImplPtr clientimpl;
  };

 
};

#endif
