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

TEST(SubscribeTest, testSyncSubscribe) {
  Hedwig::Configuration* conf = new TestServerConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);
    
  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
    
  sub.subscribe("testTopic", "mySubscriberId-1", Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
}

TEST(SubscribeTest, testSyncSubscribeAttach) {
  Hedwig::Configuration* conf = new TestServerConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);

  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
    
  ASSERT_THROW(sub.subscribe("iAmATopicWhoDoesNotExist", "mySubscriberId-2", Hedwig::SubscribeRequest::ATTACH), Hedwig::ClientException);
}

TEST(SubscribeTest, testAsyncSubscribe) {
  SimpleWaitCondition* cond1 = new SimpleWaitCondition();
  std::auto_ptr<SimpleWaitCondition> cond1ptr(cond1);

  Hedwig::Configuration* conf = new TestServerConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);

  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
   
  Hedwig::OperationCallbackPtr testcb1(new TestCallback(cond1));

  sub.asyncSubscribe("testTopic", "mySubscriberId-3", Hedwig::SubscribeRequest::CREATE_OR_ATTACH, testcb1);
    
  cond1->wait();
  ASSERT_TRUE(cond1->wasSuccess());
}
  
TEST(SubscribeTest, testAsyncSubcribeAndUnsubscribe) {
  SimpleWaitCondition* cond1 = new SimpleWaitCondition();
  std::auto_ptr<SimpleWaitCondition> cond1ptr(cond1);
  SimpleWaitCondition* cond2 = new SimpleWaitCondition();
  std::auto_ptr<SimpleWaitCondition> cond2ptr(cond2);

  Hedwig::Configuration* conf = new TestServerConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);

  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
   
  Hedwig::OperationCallbackPtr testcb1(new TestCallback(cond1));
  Hedwig::OperationCallbackPtr testcb2(new TestCallback(cond2));

  sub.asyncSubscribe("testTopic", "mySubscriberId-4", Hedwig::SubscribeRequest::CREATE_OR_ATTACH, testcb1);
  cond1->wait();
  ASSERT_TRUE(cond1->wasSuccess());
    
  sub.asyncUnsubscribe("testTopic", "mySubscriberId-4", testcb2);
  cond2->wait();
  ASSERT_TRUE(cond2->wasSuccess());
}

TEST(SubscribeTest, testAsyncSubcribeAndSyncUnsubscribe) {
  SimpleWaitCondition* cond1 = new SimpleWaitCondition();
  std::auto_ptr<SimpleWaitCondition> cond1ptr(cond1);

  Hedwig::Configuration* conf = new TestServerConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);

  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
   
  Hedwig::OperationCallbackPtr testcb1(new TestCallback(cond1));
    
  sub.asyncSubscribe("testTopic", "mySubscriberId-5", Hedwig::SubscribeRequest::CREATE_OR_ATTACH, testcb1);
  cond1->wait();
  ASSERT_TRUE(cond1->wasSuccess());

  sub.unsubscribe("testTopic", "mySubscriberId-5");
}

TEST(SubscribeTest, testAsyncSubcribeCloseSubscriptionAndThenResubscribe) {
  Hedwig::Configuration* conf = new TestServerConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);

  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
   
  sub.subscribe("testTopic", "mySubscriberId-6", Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
  sub.closeSubscription("testTopic", "mySubscriberId-6");
  sub.subscribe("testTopic", "mySubscriberId-6", Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
  sub.unsubscribe("testTopic", "mySubscriberId-6");
}

TEST(SubscribeTest, testUnsubscribeWithoutSubscribe) {
  Hedwig::Configuration* conf = new TestServerConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);
    
  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
    
  ASSERT_THROW(sub.unsubscribe("testTopic", "mySubscriberId-7"), Hedwig::NotSubscribedException);
}

TEST(SubscribeTest, testAsyncSubscribeTwice) {
  Hedwig::Configuration* conf = new TestServerConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);
  
  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();

  SimpleWaitCondition* cond1 = new SimpleWaitCondition();
  std::auto_ptr<SimpleWaitCondition> cond1ptr(cond1);
  SimpleWaitCondition* cond2 = new SimpleWaitCondition();
  std::auto_ptr<SimpleWaitCondition> cond2ptr(cond2);
  
  Hedwig::OperationCallbackPtr testcb1(new TestCallback(cond1));
  Hedwig::OperationCallbackPtr testcb2(new TestCallback(cond2));

  std::string topic("testAsyncSubscribeTwice");
  std::string subid("mysubid");

  sub.asyncSubscribe(topic, subid,
                     Hedwig::SubscribeRequest::CREATE_OR_ATTACH, testcb1);
  sub.asyncSubscribe(topic, subid,
                     Hedwig::SubscribeRequest::CREATE_OR_ATTACH, testcb2);
  cond1->wait();
  cond2->wait();

  if (cond1->wasSuccess()) {
    ASSERT_TRUE(!cond2->wasSuccess());
  } else {
    ASSERT_TRUE(cond2->wasSuccess());
  }
}

TEST(SubscribeTest, testSubscribeTwice) {
  Hedwig::Configuration* conf = new TestServerConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);
    
  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
    
  sub.subscribe("testTopic", "mySubscriberId-8", Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
  ASSERT_THROW(sub.subscribe("testTopic", "mySubscriberId-8", Hedwig::SubscribeRequest::CREATE_OR_ATTACH), Hedwig::AlreadySubscribedException);
}

TEST(SubscribeTest, testAsyncSubcribeForceAttach) {
  Hedwig::Configuration* conf = new TestServerConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);
  // client 1
  Hedwig::Client* client1 = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> client1ptr(client1);
  Hedwig::Subscriber& sub1 = client1->getSubscriber();
  // client 2
  Hedwig::Client* client2 = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> client2ptr(client2);
  Hedwig::Subscriber& sub2 = client2->getSubscriber();

  SimpleWaitCondition* cond1 = new SimpleWaitCondition();
  std::auto_ptr<SimpleWaitCondition> cond1ptr(cond1);
  Hedwig::OperationCallbackPtr testcb1(new TestCallback(cond1));

  SimpleWaitCondition* lcond1 = new SimpleWaitCondition();
  std::auto_ptr<SimpleWaitCondition> lcond1ptr(lcond1);
  Hedwig::SubscriptionListenerPtr listener1(
    new TestSubscriptionListener(lcond1, Hedwig::SUBSCRIPTION_FORCED_CLOSED));

  Hedwig::SubscriptionOptions options;
  options.set_createorattach(Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
  options.set_forceattach(true);
  options.set_enableresubscribe(false);

  sub1.addSubscriptionListener(listener1);

  sub1.asyncSubscribe("asyncSubscribeForceAttach", "mysub",
                      options, testcb1);
  cond1->wait();
  ASSERT_TRUE(cond1->wasSuccess());

  // sub2 subscribe would force close the channel of sub1
  SimpleWaitCondition* cond2 = new SimpleWaitCondition();
  std::auto_ptr<SimpleWaitCondition> cond2ptr(cond2);
  Hedwig::OperationCallbackPtr testcb2(new TestCallback(cond2));

  Hedwig::SubscriptionListenerPtr listener2(
    new TestSubscriptionListener(0, Hedwig::SUBSCRIPTION_FORCED_CLOSED));

  sub2.addSubscriptionListener(listener2);

  sub2.asyncSubscribe("asyncSubscribeForceAttach", "mysub",
                      options, testcb2);
  cond2->wait();
  ASSERT_TRUE(cond2->wasSuccess());

  // sub1 would receive the disconnect event
  lcond1->wait();

  sub1.unsubscribe("asyncSubscribeForceAttach", "mysub");
}
