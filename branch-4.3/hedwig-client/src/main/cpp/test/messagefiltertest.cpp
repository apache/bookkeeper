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

class MessageFilterConfiguration : public TestServerConfiguration {
public:
  MessageFilterConfiguration() : TestServerConfiguration() {}
  
  virtual bool getBool(const std::string& key, bool defaultVal) const {
    if (key == Configuration::SUBSCRIBER_AUTOCONSUME) {
      return false;
    } else {
      return TestServerConfiguration::getBool(key, defaultVal);
    }
  }
};
    
class ModMessageFilter : public Hedwig::ClientMessageFilter {
public:
  ModMessageFilter() : mod(0) {
  }

  virtual void setSubscriptionPreferences(const std::string& topic, const std::string& subscriberId,
                                          const Hedwig::SubscriptionPreferencesPtr& preferences) {
    if (!preferences->has_options()) {
      return;
    }

    const Hedwig::Map& userOptions = preferences->options();
    int numOpts = userOptions.entries_size();
    for (int i=0; i<numOpts; i++) {
      const Hedwig::Map_Entry& opt = userOptions.entries(i);
      const std::string& key = opt.key();
      if ("MOD" != key) {
        continue;
      }
      const std::string& value = opt.value();
      mod = atoi(value.c_str());
      break;
    }
    return; 
  }
  virtual bool testMessage(const Hedwig::Message& message) {
    int value = atoi(message.body().c_str());
    return 0 == value % mod;
  }
private:
  int mod;
};

class GapCheckingMessageHandlerCallback : public Hedwig::MessageHandlerCallback {
public:
  GapCheckingMessageHandlerCallback(Hedwig::Subscriber& sub,
                                    const int start, const int nextValue,
                                    const int gap, bool doConsume)
    : sub(sub), start(start), nextValue(nextValue), gap(gap), doConsume(doConsume) {
  }

  virtual void consume(const std::string& topic, const std::string& subscriberId,
                       const Hedwig::Message& msg, Hedwig::OperationCallbackPtr& callback) {
    boost::lock_guard<boost::mutex> lock(mutex);
    
    int value = atoi(msg.body().c_str());
    if(value > start) {
      LOG4CXX_DEBUG(logger, "received message " << value);
      if (value == nextValue) {
        nextValue += gap;
      }
    }
    callback->operationComplete();
    if (doConsume) {
      sub.consume(topic, subscriberId, msg.msgid());
    }
  }

  int nextExpected() {
    return nextValue;
  }

protected:
  boost::mutex mutex;
  Hedwig::Subscriber& sub;
  int start;
  int nextValue;
  int gap;
  bool doConsume;
};

void publishNums(Hedwig::Publisher& pub, const std::string& topic,
                 int start, int num, int M) {
  for (int i=1; i<=num; i++) {
    int value = start + i;
    int mod = value % M;

    std::stringstream valSS;
    valSS << value;

    std::stringstream modSS;
    modSS << mod;

    Hedwig::Message msg;
    msg.set_body(valSS.str());
    Hedwig::MessageHeader* header = msg.mutable_header();
    Hedwig::Map* properties = header->mutable_properties();
    Hedwig::Map_Entry* entry = properties->add_entries();
    entry->set_key("mod");
    entry->set_value(modSS.str());

    pub.publish(topic, msg);
  }
}

void receiveNumModM(Hedwig::Subscriber& sub,
                    const std::string& topic, const std::string& subid,
                    int start, int num, int M, bool consume) {
  Hedwig::SubscriptionOptions options;
  options.set_createorattach(Hedwig::SubscribeRequest::ATTACH);
  Hedwig::Map* userOptions = options.mutable_options();
  Hedwig::Map_Entry* opt = userOptions->add_entries();
  opt->set_key("MOD");

  std::stringstream modSS;
  modSS << M;
  opt->set_value(modSS.str());

  sub.subscribe(topic, subid, options);

  int base = start + M - start % M;
  int end = base + num * M;

  GapCheckingMessageHandlerCallback * cb =
    new GapCheckingMessageHandlerCallback(sub, start, base, M, consume);
  Hedwig::MessageHandlerCallbackPtr handler(cb);
  Hedwig::ClientMessageFilterPtr filter(new ModMessageFilter());

  sub.startDeliveryWithFilter(topic, subid, handler, filter);

  for (int i = 0; i < 100; i++) {
    if (cb->nextExpected() == end) {
      break;
    } else {
      sleep(1);
    }
  }
  ASSERT_TRUE(cb->nextExpected() == end);

  sub.stopDelivery(topic, subid);
  sub.closeSubscription(topic, subid);
}

TEST(MessageFilterTest, testNullMessageFilter) {
  Hedwig::Configuration* conf = new MessageFilterConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);
  
  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();

  std::string topic = "testNullMessageFilter";
  std::string subid = "myTestSubid";

  sub.subscribe(topic, subid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);

  GapCheckingMessageHandlerCallback * cb =
    new GapCheckingMessageHandlerCallback(sub, 0, 0, 0, true);
  Hedwig::MessageHandlerCallbackPtr handler(cb);
  Hedwig::ClientMessageFilterPtr filter(new ModMessageFilter());

  ASSERT_THROW(sub.startDeliveryWithFilter(topic, subid, handler,
                                           Hedwig::ClientMessageFilterPtr()),
               Hedwig::NullMessageFilterException);
  ASSERT_THROW(sub.startDeliveryWithFilter(topic, subid, 
                                           Hedwig::MessageHandlerCallbackPtr(), filter),
               Hedwig::NullMessageHandlerException);
}

TEST(MessageFilterTest, testMessageFilter) {
  Hedwig::Configuration* conf = new MessageFilterConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);
  
  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
  Hedwig::Publisher& pub = client->getPublisher();

  std::string topic = "testMessageFilter";
  std::string subid = "myTestSubid";
  sub.subscribe(topic, subid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
  sub.closeSubscription(topic, subid);

  publishNums(pub, topic, 0, 100, 2);
  receiveNumModM(sub, topic, subid, 0, 50, 2, true);
}

TEST(MessageFilterTest, testUpdateMessageFilter) {
  Hedwig::Configuration* conf = new MessageFilterConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);

  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
  Hedwig::Publisher& pub = client->getPublisher();
  
  std::string topic = "testUpdateMessageFilter";
  std::string subid = "myTestSubid";

  sub.subscribe(topic, subid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
  sub.closeSubscription(topic, subid);

  publishNums(pub, topic, 0, 100, 2);
  receiveNumModM(sub, topic, subid, 0, 50, 2, false);
  receiveNumModM(sub, topic, subid, 0, 25, 4, false);
  receiveNumModM(sub, topic, subid, 0, 33, 3, false);
}
