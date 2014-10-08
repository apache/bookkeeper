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

TEST(PublishTest, testPublishByMessage) {
  Hedwig::Configuration* conf = new TestServerConfiguration();
  Hedwig::Client* client = new Hedwig::Client(*conf);
  Hedwig::Publisher& pub = client->getPublisher();

  Hedwig::Message syncMsg;
  syncMsg.set_body("sync publish by Message");
  pub.publish("testTopic", syncMsg);

  SimpleWaitCondition* cond = new SimpleWaitCondition();
  Hedwig::OperationCallbackPtr testcb(new TestCallback(cond));
  Hedwig::Message asyncMsg;
  asyncMsg.set_body("async publish by Message");
  pub.asyncPublish("testTopic", asyncMsg, testcb);
  cond->wait();
  ASSERT_TRUE(cond->wasSuccess());
  delete cond;

  delete client;
  delete conf;
}

TEST(PublishTest, testSyncPublish) {
  Hedwig::Configuration* conf = new TestServerConfiguration();
    
  Hedwig::Client* client = new Hedwig::Client(*conf);
  Hedwig::Publisher& pub = client->getPublisher();
    
  pub.publish("testTopic", "testMessage 1");
    
  delete client;
  delete conf;
}

TEST(PublishTest, testSyncPublishWithResponse) {
  Hedwig::Configuration* conf = new TestServerConfiguration();
  
  Hedwig::Client* client = new Hedwig::Client(*conf);
  Hedwig::Publisher& pub = client->getPublisher();

  int numMsgs = 20;
  for(int i=1; i<=numMsgs; i++) {
    Hedwig::PublishResponsePtr pubResponse = pub.publish("testSyncPublishWithResponse", "testMessage " + i);
    ASSERT_EQ(i, (int)pubResponse->publishedmsgid().localcomponent());
  }
  
  delete client;
  delete conf;
}

TEST(PublishTest, testAsyncPublish) {
  SimpleWaitCondition* cond = new SimpleWaitCondition();

  Hedwig::Configuration* conf = new TestServerConfiguration();
  Hedwig::Client* client = new Hedwig::Client(*conf);
  Hedwig::Publisher& pub = client->getPublisher();
    
  Hedwig::OperationCallbackPtr testcb(new TestCallback(cond));
  pub.asyncPublish("testTopic", "async test message", testcb);
    
  cond->wait();

  ASSERT_TRUE(cond->wasSuccess());

  delete cond;
  delete client;
  delete conf;
}

TEST(PublishTest, testAsyncPublishWithResponse) {
  Hedwig::Configuration* conf = new TestServerConfiguration();
  Hedwig::Client* client = new Hedwig::Client(*conf);
  Hedwig::Publisher& pub = client->getPublisher();

  int numMsgs = 20;
  for (int i=1; i<=numMsgs; i++) {
    SimpleWaitCondition* cond = new SimpleWaitCondition();
    TestPublishResponseCallback* callback =
      new TestPublishResponseCallback(cond);
    Hedwig::PublishResponseCallbackPtr testcb(callback);
    Hedwig::Message asyncMsg;
    asyncMsg.set_body("testAsyncPublishWithResponse-" + i);
    pub.asyncPublishWithResponse("testAsyncPublishWithResponse", asyncMsg, testcb);
    
    cond->wait();

    ASSERT_TRUE(cond->wasSuccess());
    ASSERT_EQ(i, (int)callback->getResponse()->publishedmsgid().localcomponent());

    delete cond;
  }
  delete client;
  delete conf;
}

TEST(PublishTest, testMultipleAsyncPublish) {
  SimpleWaitCondition* cond1 = new SimpleWaitCondition();
  SimpleWaitCondition* cond2 = new SimpleWaitCondition();
  SimpleWaitCondition* cond3 = new SimpleWaitCondition();

  Hedwig::Configuration* conf = new TestServerConfiguration();
  Hedwig::Client* client = new Hedwig::Client(*conf);
  Hedwig::Publisher& pub = client->getPublisher();
   
  Hedwig::OperationCallbackPtr testcb1(new TestCallback(cond1));
  Hedwig::OperationCallbackPtr testcb2(new TestCallback(cond2));
  Hedwig::OperationCallbackPtr testcb3(new TestCallback(cond3));

  pub.asyncPublish("testTopic", "async test message #1", testcb1);
  pub.asyncPublish("testTopic", "async test message #2", testcb2);
  pub.asyncPublish("testTopic", "async test message #3", testcb3);
    
  cond3->wait();
  ASSERT_TRUE(cond3->wasSuccess());
  cond2->wait();
  ASSERT_TRUE(cond2->wasSuccess());
  cond1->wait();
  ASSERT_TRUE(cond1->wasSuccess());
    
  delete cond3; delete cond2; delete cond1;
  delete client;
  delete conf;
}

class UnresolvedDefaultHostCallback : public Hedwig::OperationCallback {
public:
  UnresolvedDefaultHostCallback(SimpleWaitCondition* cond) : cond(cond) {}

  virtual void operationComplete() {
    cond->setSuccess(false);
    cond->notify();
  }

  virtual void operationFailed(const std::exception& exception) {
    LOG4CXX_ERROR(logger, "Failed with exception : " << exception.what());
    cond->setSuccess(exception.what() == Hedwig::HostResolutionException().what());
    cond->notify();
  }

private:
  SimpleWaitCondition *cond;
};

TEST(PublishTest, testPublishWithUnresolvedDefaultHost) {
  std::string invalidHost("");
  Hedwig::Configuration* conf = new TestServerConfiguration(invalidHost);
  
  SimpleWaitCondition* cond = new SimpleWaitCondition();
  Hedwig::Client* client = new Hedwig::Client(*conf);
  Hedwig::Publisher& pub = client->getPublisher();
  Hedwig::OperationCallbackPtr testcb(new UnresolvedDefaultHostCallback(cond));

  pub.asyncPublish("testTopic", "testPublishWithUnresolvedDefaultHost", testcb);
  
  cond->wait();
  ASSERT_TRUE(cond->wasSuccess());
  
  delete cond;
  delete client;
  delete conf;
}
  /*  void simplePublish() {
    LOG4CXX_DEBUG(logger, ">>> simplePublish");
    SimpleWaitCondition* cond = new SimpleWaitCondition();

    Hedwig::Configuration* conf = new Configuration1();
    Hedwig::Client* client = new Hedwig::Client(*conf);
    Hedwig::Publisher& pub = client->getPublisher();
    
    Hedwig::OperationCallbackPtr testcb(new TestCallback(cond));
    pub.asyncPublish("foobar", "barfoo", testcb);
    
    LOG4CXX_DEBUG(logger, "wait for response");
    cond->wait();
    delete cond;
    LOG4CXX_DEBUG(logger, "got response");
    

    delete client;
    delete conf;
    LOG4CXX_DEBUG(logger, "<<< simplePublish");
  }

  class MyMessageHandler : public Hedwig::MessageHandlerCallback {
  public:
    MyMessageHandler(SimpleWaitCondition* cond) : cond(cond) {}

    void consume(const std::string& topic, const std::string& subscriberId, const Hedwig::Message& msg, Hedwig::OperationCallbackPtr& callback) {
      LOG4CXX_DEBUG(logger, "Topic: " << topic << "  subscriberId: " << subscriberId);
      LOG4CXX_DEBUG(logger, " Message: " << msg.body());
      
      callback->operationComplete();
      cond->setTrue();
      cond->signal();
    }
  private:
    SimpleWaitCondition* cond;
    };*/
  /*
  void simplePublishAndSubscribe() {
    SimpleWaitCondition* cond1 = new SimpleWaitCondition();
    SimpleWaitCondition* cond2 = new SimpleWaitCondition();
    SimpleWaitCondition* cond3 = new SimpleWaitCondition();

    Hedwig::Configuration* conf = new Configuration1();
    Hedwig::Client* client = new Hedwig::Client(*conf);
    Hedwig::Publisher& pub = client->getPublisher();
    Hedwig::Subscriber& sub = client->getSubscriber();
    
    std::string topic("foobar");
    std::string sid("mysubscriber");
    Hedwig::OperationCallbackPtr testcb1(new TestCallback(cond1));
    sub.asyncSubscribe(topic, sid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH, testcb1);
    Hedwig::MessageHandlerCallbackPtr messagecb(new MyMessageHandler(cond2));
    sub.startDelivery(topic, sid, messagecb);
    cond1->wait();
    
    Hedwig::OperationCallbackPtr testcb2(new TestCallback(cond3));
    pub.asyncPublish("foobar", "barfoo", testcb2);
    cond3->wait();
    cond2->wait();

    delete cond1;
    delete cond3;
    delete cond2;

    delete client;
    delete conf;
  }

  void publishAndSubscribeWithRedirect() {
    SimpleWaitCondition* cond1 = new SimpleWaitCondition();
    SimpleWaitCondition* cond2 = new SimpleWaitCondition();
    SimpleWaitCondition* cond3 = new SimpleWaitCondition();
    SimpleWaitCondition* cond4 = new SimpleWaitCondition();

    Hedwig::Configuration* publishconf = new Configuration1();
    Hedwig::Configuration* subscribeconf = new Configuration2();

    Hedwig::Client* publishclient = new Hedwig::Client(*publishconf);
    Hedwig::Publisher& pub = publishclient->getPublisher();

    Hedwig::Client* subscribeclient = new Hedwig::Client(*subscribeconf);
    Hedwig::Subscriber& sub = subscribeclient->getSubscriber();
    
    LOG4CXX_DEBUG(logger, "publishing");
    Hedwig::OperationCallbackPtr testcb2(new TestCallback(cond3));
    pub.asyncPublish("foobar", "barfoo", testcb2);
    cond3->wait();
    
    LOG4CXX_DEBUG(logger, "Subscribing");
    std::string topic("foobar");
    std::string sid("mysubscriber");
    Hedwig::OperationCallbackPtr testcb1(new TestCallback(cond1));
    sub.asyncSubscribe(topic, sid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH, testcb1);
    LOG4CXX_DEBUG(logger, "Starting delivery");
    Hedwig::MessageHandlerCallbackPtr messagecb(new MyMessageHandler(cond2));
    sub.startDelivery(topic, sid, messagecb);

    LOG4CXX_DEBUG(logger, "Subscribe wait");
    cond1->wait();

    Hedwig::OperationCallbackPtr testcb3(new TestCallback(cond4));
    pub.asyncPublish("foobar", "barfoo", testcb3);
    cond4->wait();


    LOG4CXX_DEBUG(logger, "Delivery wait");

    cond2->wait();

    sub.stopDelivery(topic, sid);

    delete cond1;
    delete cond3;
    delete cond2;
    delete cond4;

    delete subscribeclient;
    delete publishclient;
    delete publishconf;
    delete subscribeconf;
    }*/
