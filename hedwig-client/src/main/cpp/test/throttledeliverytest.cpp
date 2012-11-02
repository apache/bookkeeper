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

class ThrottleDeliveryConfiguration : public TestServerConfiguration {
public:
  ThrottleDeliveryConfiguration() : TestServerConfiguration() {}
  
  virtual bool getBool(const std::string& key, bool defaultVal) const {
    if (key == Configuration::SUBSCRIBER_AUTOCONSUME) {
      return false;
    } else {
      return TestServerConfiguration::getBool(key, defaultVal);
    }
  }
};
    
class ThrottleDeliveryMessageHandlerCallback : public Hedwig::MessageHandlerCallback {
public:
  ThrottleDeliveryMessageHandlerCallback(Hedwig::Subscriber& sub,
                                         const int start, const int end,
                                         const int expectedToThrottle,
                                         SimpleWaitCondition& throttleLatch,
                                         SimpleWaitCondition& nonThrottleLatch)
    : sub(sub), next(start), end(end), expectedToThrottle(expectedToThrottle),
      throttleLatch(throttleLatch), nonThrottleLatch(nonThrottleLatch) {
  }

  virtual void consume(const std::string& topic, const std::string& subscriberId,
                       const Hedwig::Message& msg, Hedwig::OperationCallbackPtr& callback) {
    const int value = atoi(msg.body().c_str());
    LOG4CXX_DEBUG(logger, "received message " << value);
    boost::lock_guard<boost::mutex> lock(mutex);
    if (value == next) {
      ++next;
    } else {
      LOG4CXX_ERROR(logger, "Did not receive expected value " << next << ", got " << value);
      next = 0;
      throttleLatch.setSuccess(false);
      throttleLatch.notify();
      nonThrottleLatch.setSuccess(false);
      nonThrottleLatch.notify();
    }
    if (next == expectedToThrottle + 2) {
      throttleLatch.setSuccess(true);
      throttleLatch.notify();
    } else if (next == end + 1) {
      nonThrottleLatch.setSuccess(true);
      nonThrottleLatch.notify();
    }
    callback->operationComplete();
    if (next > expectedToThrottle + 1) {
      sub.consume(topic, subscriberId, msg.msgid());
    }
  }

  int nextExpected() {
    boost::lock_guard<boost::mutex> lock(mutex);
    return next;
  }

protected:
  Hedwig::Subscriber& sub;
  boost::mutex mutex;
  int next;
  const int end;
  const int expectedToThrottle;
  SimpleWaitCondition& throttleLatch;
  SimpleWaitCondition& nonThrottleLatch;
};

void throttleX(Hedwig::Publisher& pub, Hedwig::Subscriber& sub,
               const std::string& topic, const std::string& subid, int X) {
  for (int i = 1; i <= 3*X; i++) {
    std::stringstream oss;
    oss << i;
    pub.publish(topic, oss.str());
  }

  sub.subscribe(topic, subid, Hedwig::SubscribeRequest::ATTACH);

  SimpleWaitCondition throttleLatch, nonThrottleLatch;

  ThrottleDeliveryMessageHandlerCallback* cb =
    new ThrottleDeliveryMessageHandlerCallback(sub, 1, 3*X, X, throttleLatch,
                                               nonThrottleLatch);
  Hedwig::MessageHandlerCallbackPtr handler(cb);
  sub.startDelivery(topic, subid, handler);

  throttleLatch.timed_wait(3000);
  ASSERT_TRUE(!throttleLatch.wasSuccess());
  ASSERT_EQ(X + 1, cb->nextExpected());

  // consume messages to not throttle it
  for (int i=1; i<=X; i++) {
    Hedwig::MessageSeqId msgid;
    msgid.set_localcomponent(i);
    sub.consume(topic, subid, msgid);
  }

  nonThrottleLatch.timed_wait(10000);
  ASSERT_TRUE(nonThrottleLatch.wasSuccess());
  ASSERT_EQ(3*X + 1, cb->nextExpected());

  sub.stopDelivery(topic, subid);
  sub.closeSubscription(topic, subid);
}

TEST(ThrottleDeliveryTest, testThrottleDelivery) {
  Hedwig::Configuration* conf = new ThrottleDeliveryConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);
  
  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
  Hedwig::Publisher& pub = client->getPublisher();

  int throttleValue = 10;
  std::string topic = "testThrottleDelivery";
  std::string subid = "testSubId";
  Hedwig::SubscriptionOptions options;
  options.set_createorattach(Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
  options.set_messagewindowsize(throttleValue);
  sub.subscribe(topic, subid, options);
  sub.closeSubscription(topic, subid);
  throttleX(pub, sub, topic, subid, throttleValue);
}
