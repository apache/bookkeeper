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

class MultiplexConfiguration : public TestServerConfiguration {
public:
  MultiplexConfiguration() : TestServerConfiguration() {}
  
  virtual bool getBool(const std::string& key, bool defaultVal) const {
    if (key == Configuration::SUBSCRIBER_AUTOCONSUME) {
      return false;
    } else if (key == Configuration::SUBSCRIPTION_CHANNEL_SHARING_ENABLED) {    
      return true;
    } else {
      return TestServerConfiguration::getBool(key, defaultVal);
    }
  }
};
    
class MultiplexMessageHandlerCallback : public Hedwig::MessageHandlerCallback {
public:
  MultiplexMessageHandlerCallback(Hedwig::Subscriber& sub,
                                  const int start, const int numMsgsAtFirstRun,
                                  const bool receiveSecondRun,
                                  const int numMsgsAtSecondRun)
    : sub(sub), next(start), start(start), numMsgsAtFirstRun(numMsgsAtFirstRun),
      numMsgsAtSecondRun(numMsgsAtSecondRun), receiveSecondRun(receiveSecondRun) {
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
      firstLatch.setSuccess(false);
      firstLatch.notify();
      secondLatch.setSuccess(false);
      secondLatch.notify();
    }
    if (numMsgsAtFirstRun + start == next) {
      firstLatch.setSuccess(true);
      firstLatch.notify();
    }
    if (receiveSecondRun) {
      if (numMsgsAtFirstRun + numMsgsAtSecondRun + start == next) {
        secondLatch.setSuccess(true);
        secondLatch.notify();
      }
    } else {
      if (numMsgsAtFirstRun + start + 1 == next) {
        secondLatch.setSuccess(true);
        secondLatch.notify();
      }
    }
    callback->operationComplete();
    sub.consume(topic, subscriberId, msg.msgid());
  }

  void checkFirstRun() {
    firstLatch.timed_wait(10000);
    ASSERT_TRUE(firstLatch.wasSuccess());
    ASSERT_EQ(numMsgsAtFirstRun + start, next);
  }

  void checkSecondRun() {
    if (receiveSecondRun) {
      secondLatch.timed_wait(10000);
      ASSERT_TRUE(secondLatch.wasSuccess());
      ASSERT_EQ(numMsgsAtFirstRun + numMsgsAtSecondRun + start, next);
    } else {
      secondLatch.timed_wait(3000);
      ASSERT_TRUE(!secondLatch.wasSuccess());
      ASSERT_EQ(numMsgsAtFirstRun + start, next);
    }
  }

protected:
  Hedwig::Subscriber& sub;
  boost::mutex mutex;
  int next;
  const int start;
  const int numMsgsAtFirstRun;
  const int numMsgsAtSecondRun;
  SimpleWaitCondition firstLatch;
  SimpleWaitCondition secondLatch;
  const bool receiveSecondRun;
};

class MultiplexThrottleDeliveryMessageHandlerCallback : public Hedwig::MessageHandlerCallback {
public:
  MultiplexThrottleDeliveryMessageHandlerCallback(Hedwig::Subscriber& sub,
                                         const int start, const int numMsgs,
                                         const bool enableThrottle,
                                         const int numMsgsThrottle)
    : sub(sub), next(start), start(start), numMsgs(numMsgs),
      numMsgsThrottle(numMsgsThrottle), enableThrottle(enableThrottle) {
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
    if (next == numMsgsThrottle + start + 1) {
      throttleLatch.setSuccess(true);
      throttleLatch.notify();
    } else if (next == numMsgs + 1) {
      nonThrottleLatch.setSuccess(true);
      nonThrottleLatch.notify();
    }
    callback->operationComplete();
    if (enableThrottle) {
      if (next > numMsgsThrottle + start) {
        sub.consume(topic, subscriberId, msg.msgid());
      }
    } else {
      sub.consume(topic, subscriberId, msg.msgid());
    }
  }

  void checkThrottle() {
    if (enableThrottle) {
      throttleLatch.timed_wait(3000);
      ASSERT_TRUE(!throttleLatch.wasSuccess());
      ASSERT_EQ(numMsgsThrottle + start, next);
    } else {
      throttleLatch.timed_wait(10000);
      ASSERT_TRUE(throttleLatch.wasSuccess());
      nonThrottleLatch.timed_wait(10000);
      ASSERT_TRUE(nonThrottleLatch.wasSuccess());
      ASSERT_EQ(numMsgs + start, next);
    }
  }

  void checkAfterThrottle() {
    if (enableThrottle) {
      nonThrottleLatch.timed_wait(10000);
      ASSERT_TRUE(nonThrottleLatch.wasSuccess());
      ASSERT_EQ(numMsgs + start, next);
    }
  }

protected:
  Hedwig::Subscriber& sub;
  boost::mutex mutex;
  int next;
  const int start;
  const int numMsgs;
  const int numMsgsThrottle;
  const bool enableThrottle;
  SimpleWaitCondition throttleLatch;
  SimpleWaitCondition nonThrottleLatch;
};

TEST(MultiplexTest, testStopDelivery) {
  Hedwig::Configuration* conf = new MultiplexConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);
  
  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
  Hedwig::Publisher& pub = client->getPublisher();

  const int numMsgs = 20;
  std::string topic1 = "testStopDelivery-1";
  std::string subid1 = "mysubid-1";
  std::string topic2 = "testStopDelivery-2";
  std::string subid2 = "mysubid-2";

  MultiplexMessageHandlerCallback * cb11 =
    new MultiplexMessageHandlerCallback(sub, 1, numMsgs, true, numMsgs);
  MultiplexMessageHandlerCallback * cb12 =
    new MultiplexMessageHandlerCallback(sub, 1, numMsgs, false, 0);
  MultiplexMessageHandlerCallback * cb21 =
    new MultiplexMessageHandlerCallback(sub, 1, numMsgs, false, 0);
  MultiplexMessageHandlerCallback * cb22 =
    new MultiplexMessageHandlerCallback(sub, 1, numMsgs, true, numMsgs);

  Hedwig::MessageHandlerCallbackPtr handler11(cb11);
  Hedwig::MessageHandlerCallbackPtr handler12(cb12);
  Hedwig::MessageHandlerCallbackPtr handler21(cb21);
  Hedwig::MessageHandlerCallbackPtr handler22(cb22);

  sub.subscribe(topic1, subid1, Hedwig::SubscribeRequest::CREATE);
  sub.subscribe(topic1, subid2, Hedwig::SubscribeRequest::CREATE);
  sub.subscribe(topic2, subid1, Hedwig::SubscribeRequest::CREATE);
  sub.subscribe(topic2, subid2, Hedwig::SubscribeRequest::CREATE);

  // start deliveries
  sub.startDelivery(topic1, subid1, handler11);
  sub.startDelivery(topic1, subid2, handler12);
  sub.startDelivery(topic2, subid1, handler21);
  sub.startDelivery(topic2, subid2, handler22);

  // first publish
  for (int i = 1; i <= numMsgs; i++) {
    std::stringstream oss;
    oss << i;
    pub.publish(topic1, oss.str());
    pub.publish(topic2, oss.str());
  }

  // check first run
  cb11->checkFirstRun();
  cb12->checkFirstRun();
  cb21->checkFirstRun();
  cb22->checkFirstRun();

  // stop delivery for <topic1, subscriber2> and <topic2, subscriber1>
  sub.stopDelivery(topic1, subid2);
  sub.stopDelivery(topic2, subid1);

  // second publish
  for (int i = numMsgs+1; i <= 2*numMsgs; i++) {
    std::stringstream oss;
    oss << i;
    pub.publish(topic1, oss.str());
    pub.publish(topic2, oss.str());
  }

  cb11->checkSecondRun();
  cb12->checkSecondRun();
  cb21->checkSecondRun();
  cb22->checkSecondRun();
}

TEST(MultiplexTest, testCloseSubscription) {
  Hedwig::Configuration* conf = new MultiplexConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);
  
  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
  Hedwig::Publisher& pub = client->getPublisher();

  const int numMsgs = 20;
  std::string topic1 = "testCloseSubscription-1";
  std::string subid1 = "mysubid-1";
  std::string topic2 = "testCloseSubscription-2";
  std::string subid2 = "mysubid-2";

  MultiplexMessageHandlerCallback * cb11 =
    new MultiplexMessageHandlerCallback(sub, 1, numMsgs, true, numMsgs);
  MultiplexMessageHandlerCallback * cb12 =
    new MultiplexMessageHandlerCallback(sub, 1, numMsgs, false, 0);
  MultiplexMessageHandlerCallback * cb21 =
    new MultiplexMessageHandlerCallback(sub, 1, numMsgs, false, 0);
  MultiplexMessageHandlerCallback * cb22 =
    new MultiplexMessageHandlerCallback(sub, 1, numMsgs, true, numMsgs);

  Hedwig::MessageHandlerCallbackPtr handler11(cb11);
  Hedwig::MessageHandlerCallbackPtr handler12(cb12);
  Hedwig::MessageHandlerCallbackPtr handler21(cb21);
  Hedwig::MessageHandlerCallbackPtr handler22(cb22);

  sub.subscribe(topic1, subid1, Hedwig::SubscribeRequest::CREATE);
  sub.subscribe(topic1, subid2, Hedwig::SubscribeRequest::CREATE);
  sub.subscribe(topic2, subid1, Hedwig::SubscribeRequest::CREATE);
  sub.subscribe(topic2, subid2, Hedwig::SubscribeRequest::CREATE);

  // start deliveries
  sub.startDelivery(topic1, subid1, handler11);
  sub.startDelivery(topic1, subid2, handler12);
  sub.startDelivery(topic2, subid1, handler21);
  sub.startDelivery(topic2, subid2, handler22);

  // first publish
  for (int i = 1; i <= numMsgs; i++) {
    std::stringstream oss;
    oss << i;
    pub.publish(topic1, oss.str());
    pub.publish(topic2, oss.str());
  }

  // check first run
  cb11->checkFirstRun();
  cb12->checkFirstRun();
  cb21->checkFirstRun();
  cb22->checkFirstRun();

  // close subscription for <topic1, subscriber2> and <topic2, subscriber1>
  sub.closeSubscription(topic1, subid2);
  sub.closeSubscription(topic2, subid1);

  // second publish
  for (int i = numMsgs+1; i <= 2*numMsgs; i++) {
    std::stringstream oss;
    oss << i;
    pub.publish(topic1, oss.str());
    pub.publish(topic2, oss.str());
  }

  cb11->checkSecondRun();
  cb12->checkSecondRun();
  cb21->checkSecondRun();
  cb22->checkSecondRun();
}

TEST(MultiplexTest, testThrottle) {
  Hedwig::Configuration* conf = new MultiplexConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);
  
  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
  Hedwig::Publisher& pub = client->getPublisher();

  const int numMsgs = 10;
  std::string topic1 = "testThrottle-1";
  std::string subid1 = "mysubid-1";
  std::string topic2 = "testThrottle-2";
  std::string subid2 = "mysubid-2";

  MultiplexThrottleDeliveryMessageHandlerCallback * cb11 =
    new MultiplexThrottleDeliveryMessageHandlerCallback(sub, 1, 3*numMsgs, false, numMsgs);
  MultiplexThrottleDeliveryMessageHandlerCallback * cb12 =
    new MultiplexThrottleDeliveryMessageHandlerCallback(sub, 1, 3*numMsgs, true, numMsgs);
  MultiplexThrottleDeliveryMessageHandlerCallback * cb21 =
    new MultiplexThrottleDeliveryMessageHandlerCallback(sub, 1, 3*numMsgs, true, numMsgs);
  MultiplexThrottleDeliveryMessageHandlerCallback * cb22 =
    new MultiplexThrottleDeliveryMessageHandlerCallback(sub, 1, 3*numMsgs, false, numMsgs);

  Hedwig::MessageHandlerCallbackPtr handler11(cb11);
  Hedwig::MessageHandlerCallbackPtr handler12(cb12);
  Hedwig::MessageHandlerCallbackPtr handler21(cb21);
  Hedwig::MessageHandlerCallbackPtr handler22(cb22);

  Hedwig::SubscriptionOptions options;
  options.set_createorattach(Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
  options.set_messagewindowsize(numMsgs);

  sub.subscribe(topic1, subid1, options);
  sub.subscribe(topic1, subid2, options);
  sub.subscribe(topic2, subid1, options);
  sub.subscribe(topic2, subid2, options);

  // start deliveries
  sub.startDelivery(topic1, subid1, handler11);
  sub.startDelivery(topic1, subid2, handler12);
  sub.startDelivery(topic2, subid1, handler21);
  sub.startDelivery(topic2, subid2, handler22);

  // first publish
  for (int i = 1; i <= 3*numMsgs; i++) {
    std::stringstream oss;
    oss << i;
    pub.publish(topic1, oss.str());
    pub.publish(topic2, oss.str());
  }

  // check first run
  cb11->checkThrottle();
  cb12->checkThrottle();
  cb21->checkThrottle();
  cb22->checkThrottle();

  // consume messages to not throttle them
  for (int i=1; i<=numMsgs; i++) {
    Hedwig::MessageSeqId msgid;
    msgid.set_localcomponent(i);
    sub.consume(topic1, subid2, msgid);
    sub.consume(topic2, subid1, msgid);
  }

  cb11->checkAfterThrottle();
  cb12->checkAfterThrottle();
  cb21->checkAfterThrottle();
  cb22->checkAfterThrottle();
}
