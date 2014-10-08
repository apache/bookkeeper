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

#include "gtest/gtest.h"

#include "../lib/clientimpl.h"
#include <hedwig/exceptions.h>
#include <hedwig/callback.h>
#include <stdexcept>
#include <pthread.h>

#include <log4cxx/logger.h>

#include "util.h"

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("hedwig."__FILE__));

class MessageBoundConfiguration : public TestServerConfiguration {
public:
  MessageBoundConfiguration() : TestServerConfiguration() {}
    
  virtual int getInt(const std::string& key, int defaultVal) const {
    if (key == Configuration::SUBSCRIPTION_MESSAGE_BOUND) {
      return 5;
    }
    return TestServerConfiguration::getInt(key, defaultVal);
  }
};
    
class MessageBoundOrderCheckingMessageHandlerCallback : public Hedwig::MessageHandlerCallback {
public:
  MessageBoundOrderCheckingMessageHandlerCallback(const int nextExpectedMsg)
    : nextExpectedMsg(nextExpectedMsg) {
  }

  virtual void consume(const std::string& topic, const std::string& subscriberId,
                       const Hedwig::Message& msg, Hedwig::OperationCallbackPtr& callback) {
    boost::lock_guard<boost::mutex> lock(mutex);
      
    int thisMsg = atoi(msg.body().c_str());
    LOG4CXX_DEBUG(logger, "received message " << thisMsg);
    if (thisMsg == nextExpectedMsg) {
      nextExpectedMsg++;
    }
    // checking msgId
    callback->operationComplete();
  }

  int nextExpected() {
    return nextExpectedMsg;
  }

protected:
  boost::mutex mutex;
  int nextExpectedMsg;
};

void sendXExpectLastY(Hedwig::Publisher& pub, Hedwig::Subscriber& sub, const std::string& topic, 
                      const std::string& subid, int X, int Y) {
  for (int i = 0; i < X;) {
    std::stringstream oss;
    oss << i;
    try {
      pub.publish(topic, oss.str());
      ++i;
    } catch (std::exception &e) {
      LOG4CXX_WARN(logger, "Exception when publishing message " << i << " : "
                           << e.what());
    }
  }

  sub.subscribe(topic, subid, Hedwig::SubscribeRequest::ATTACH);

  MessageBoundOrderCheckingMessageHandlerCallback* cb =
    new MessageBoundOrderCheckingMessageHandlerCallback(X - Y);

  Hedwig::MessageHandlerCallbackPtr handler(cb);
  sub.startDelivery(topic, subid, handler);

  for (int i = 0; i < 100; i++) {
    if (cb->nextExpected() == X) {
      break;
    } else {
      sleep(1);
    }
  }
  ASSERT_TRUE(cb->nextExpected() == X);

  sub.stopDelivery(topic, subid);
  sub.closeSubscription(topic, subid);
}

TEST(MessageBoundTest, testMessageBound) {
  Hedwig::Configuration* conf = new MessageBoundConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);
    
  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
  Hedwig::Publisher& pub = client->getPublisher();

  std::string topic = "testMessageBound";
  std::string subid = "testSubId";
  sub.subscribe(topic, subid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
  sub.closeSubscription(topic, subid);

  sendXExpectLastY(pub, sub, topic, subid, 100, 5);
}

TEST(MessageBoundTest, testMultipleSubscribers) {
  Hedwig::Configuration* conf = new TestServerConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);

  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
  Hedwig::Publisher& pub = client->getPublisher();
    
  Hedwig::SubscriptionOptions options5;
  options5.set_messagebound(5);
  options5.set_createorattach(Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
  Hedwig::SubscriptionOptions options20;
  options20.set_messagebound(20);
  options20.set_createorattach(Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
  Hedwig::SubscriptionOptions optionsUnlimited;
  optionsUnlimited.set_createorattach(Hedwig::SubscribeRequest::CREATE_OR_ATTACH);

  std::string topic = "testMultipleSubscribers";
  std::string subid5 = "testSubId5";
  std::string subid20 = "testSubId20";
  std::string subidUnlimited = "testSubIdUnlimited";

  sub.subscribe(topic, subid5, options5);
  sub.closeSubscription(topic, subid5);

  sendXExpectLastY(pub, sub, topic, subid5, 1000, 5);

  sub.subscribe(topic, subid20, options20);
  sub.closeSubscription(topic, subid20);
  sendXExpectLastY(pub, sub, topic, subid20, 1000, 20);

  sub.subscribe(topic, subidUnlimited, optionsUnlimited);
  sub.closeSubscription(topic, subidUnlimited);

  sendXExpectLastY(pub, sub, topic, subidUnlimited, 1000, 1000);
  sub.unsubscribe(topic, subidUnlimited);

  sendXExpectLastY(pub, sub, topic, subid20, 1000, 20);
  sub.unsubscribe(topic, subid20);

  sendXExpectLastY(pub, sub, topic, subid5, 1000, 5);
  sub.unsubscribe(topic, subid5);
}

TEST(MessageBoundTest, testUpdateMessageBound) {
  Hedwig::Configuration* conf = new TestServerConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);

  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
  Hedwig::Publisher& pub = client->getPublisher();

  Hedwig::SubscriptionOptions options5;
  options5.set_messagebound(5);
  options5.set_createorattach(Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
  Hedwig::SubscriptionOptions options20;
  options20.set_messagebound(20);
  options20.set_createorattach(Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
  Hedwig::SubscriptionOptions options10;
  options10.set_messagebound(10);
  options10.set_createorattach(Hedwig::SubscribeRequest::CREATE_OR_ATTACH);

  std::string topic = "testUpdateMessageBound";
  std::string subid = "updateSubId";

  sub.subscribe(topic, subid, options5);
  sub.closeSubscription(topic, subid);
  sendXExpectLastY(pub, sub, topic, subid, 50, 5);

  // update bound to 20
  sub.subscribe(topic, subid, options20);
  sub.closeSubscription(topic, subid);
  sendXExpectLastY(pub, sub, topic, subid, 50, 20);

  // update bound to 10
  sub.subscribe(topic, subid, options10);
  sub.closeSubscription(topic, subid);
  sendXExpectLastY(pub, sub, topic, subid, 50, 10);

  // message bound is not provided, no update
  sub.subscribe(topic, subid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
  sub.closeSubscription(topic, subid);
  sendXExpectLastY(pub, sub, topic, subid, 50, 10);
}
