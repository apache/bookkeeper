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

#include <sstream>

#include <cppunit/Test.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>
#include <boost/thread/mutex.hpp>

#include "../lib/clientimpl.h"
#include <hedwig/exceptions.h>
#include <hedwig/callback.h>
#include <stdexcept>
#include <pthread.h>

#include <log4cxx/logger.h>

#include "util.h"

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("hedwig."__FILE__));

class PubSubTestSuite : public CppUnit::TestFixture {
private:
  CPPUNIT_TEST_SUITE( PubSubTestSuite );
  CPPUNIT_TEST(testPubSubOrderChecking);
  CPPUNIT_TEST(testRandomDelivery);
  CPPUNIT_TEST(testPubSubContinuousOverClose);
  //  CPPUNIT_TEST(testPubSubContinuousOverServerDown);
  CPPUNIT_TEST(testMultiTopic);
  CPPUNIT_TEST(testBigMessage);
  CPPUNIT_TEST(testMultiTopicMultiSubscriber);
  CPPUNIT_TEST(testPubSubInMultiDispatchThreads);
  CPPUNIT_TEST_SUITE_END();

public:
  PubSubTestSuite() {
  }

  ~PubSubTestSuite() {
  }

  void setUp()
  {
  }
  
  void tearDown() 
  {
  }

  class MyMessageHandlerCallback : public Hedwig::MessageHandlerCallback {
  public:
    MyMessageHandlerCallback(const std::string& topic, const std::string& subscriberId) : messagesReceived(0), topic(topic), subscriberId(subscriberId) {
      
    }

    virtual void consume(const std::string& topic, const std::string& subscriberId, const Hedwig::Message& msg, Hedwig::OperationCallbackPtr& callback) {
      if (topic == this->topic && subscriberId == this->subscriberId) {
	boost::lock_guard<boost::mutex> lock(mutex);
      
	messagesReceived++;
	lastMessage = msg.body();
	callback->operationComplete();
      }
    }
    
    std::string getLastMessage() {
      boost::lock_guard<boost::mutex> lock(mutex);
      std::string s = lastMessage;
      return s;
    }

    int numMessagesReceived() {
      boost::lock_guard<boost::mutex> lock(mutex);
      int i = messagesReceived;
      return i;
    }    
    
  protected:
    boost::mutex mutex;
    int messagesReceived;
    std::string lastMessage;
    std::string topic;
    std::string subscriberId;
  };

  // order checking callback
  class MyOrderCheckingMessageHandlerCallback : public Hedwig::MessageHandlerCallback {
  public:
    MyOrderCheckingMessageHandlerCallback(const std::string& topic, const std::string& subscriberId, const int startMsgId, const int sleepTimeInConsume)
      : messagesReceived(0), topic(topic), subscriberId(subscriberId), startMsgId(startMsgId), 
        isInOrder(true), sleepTimeInConsume(sleepTimeInConsume) {
    }

    virtual void consume(const std::string& topic, const std::string& subscriberId,
                         const Hedwig::Message& msg, Hedwig::OperationCallbackPtr& callback) {
      if (topic == this->topic && subscriberId == this->subscriberId) {
        boost::lock_guard<boost::mutex> lock(mutex);
            
        messagesReceived++;

        int newMsgId = atoi(msg.body().c_str());
        // checking msgId
        LOG4CXX_DEBUG(logger, "received message " << newMsgId);
        if (startMsgId >= 0) { // need to check ordering if start msg id is larger than 0
          if (isInOrder) {
            if (newMsgId != startMsgId + 1) {
              LOG4CXX_ERROR(logger, "received out-of-order message : expected " << (startMsgId + 1) << ", actual " << newMsgId);
              isInOrder = false;
            } else {
              startMsgId = newMsgId;
            }
          }
        } else { // we set first msg id as startMsgId when startMsgId is -1
          startMsgId = newMsgId;
        }
        callback->operationComplete();
        sleep(sleepTimeInConsume);
      }
    }
    
    int numMessagesReceived() {
      boost::lock_guard<boost::mutex> lock(mutex);
      int i = messagesReceived;
      return i;
    }    

    bool inOrder() {
      boost::lock_guard<boost::mutex> lock(mutex);
      return isInOrder;
    }
    
  protected:
    boost::mutex mutex;
    int messagesReceived;
    std::string topic;
    std::string subscriberId;
    int startMsgId;
    bool isInOrder;
    int sleepTimeInConsume;
  };

  // Publisher integer until finished
  class IntegerPublisher {
  public:
    IntegerPublisher(const std::string &topic, int startMsgId, int numMsgs, int sleepTime, Hedwig::Publisher &pub, long runTime)
      : topic(topic), startMsgId(startMsgId), numMsgs(numMsgs), sleepTime(sleepTime), pub(pub), running(true), runTime(runTime) {
    }

    void operator()() {
      int i = 1;
      long beginTime = curTime();
      long elapsedTime = 0;

      while (running) {
        try {
          int msg = startMsgId + i;
          std::stringstream ss;
          ss << msg;
          pub.publish(topic, ss.str());
          sleep(sleepTime);
          if (numMsgs > 0 && i >= numMsgs) {
            running = false;
          } else {
            if (i % 100 == 0 &&
                (elapsedTime = (curTime() - beginTime)) >= runTime) {
              LOG4CXX_DEBUG(logger, "Elapsed time : " << elapsedTime);
              running = false;
            }
          }
          ++i;
        } catch (std::exception &e) {
          LOG4CXX_WARN(logger, "Exception when publishing messages : " << e.what());
        }
      } 
    }

    long curTime() {
      struct timeval tv;
      long mtime;
      gettimeofday(&tv, NULL);
      mtime = tv.tv_sec * 1000 + tv.tv_usec / 1000.0 + 0.5;
      return mtime;
    }

  private:
    std::string topic;
    int startMsgId;
    int numMsgs;
    int sleepTime;
    Hedwig::Publisher& pub;
    bool running;
    long runTime;
  };

  // test startDelivery / stopDelivery randomly
  void testRandomDelivery() {
    std::string topic = "randomDeliveryTopic";
    std::string subscriber = "mysub-randomDelivery";

    int nLoops = 300;
    int sleepTimePerLoop = 1;
    int syncTimeout = 10000;

    Hedwig::Configuration* conf = new TestServerConfiguration(syncTimeout);
    std::auto_ptr<Hedwig::Configuration> confptr(conf);

    Hedwig::Client* client = new Hedwig::Client(*conf);
    std::auto_ptr<Hedwig::Client> clientptr(client);

    Hedwig::Subscriber& sub = client->getSubscriber();
    Hedwig::Publisher& pub = client->getPublisher();

    // subscribe topic
    sub.subscribe(topic, subscriber, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);

    // start thread to publish message
    IntegerPublisher intPublisher = IntegerPublisher(topic, 0, 0, 0, pub, nLoops * sleepTimePerLoop * 1000);
    boost::thread pubThread(intPublisher);

    // start random delivery
    MyOrderCheckingMessageHandlerCallback* cb =
      new MyOrderCheckingMessageHandlerCallback(topic, subscriber, 0, 0);
    Hedwig::MessageHandlerCallbackPtr handler(cb);

    for (int i = 0; i < nLoops; i++) {
      LOG4CXX_DEBUG(logger, "Randomly Delivery : " << i);
      sub.startDelivery(topic, subscriber, handler);
      // sleep random time
      sleep(sleepTimePerLoop);
      sub.stopDelivery(topic, subscriber);
      CPPUNIT_ASSERT(cb->inOrder());
    }
    
    pubThread.join();
  }

  // check message ordering
  void testPubSubOrderChecking() {
    std::string topic = "orderCheckingTopic";
    std::string sid = "mysub-0";

    int numMessages = 5;
    int sleepTimeInConsume = 1;
    // sync timeout
    int syncTimeout = 10000;

    // in order to guarantee message order, message queue should be locked
    // so message received in io thread would be blocked, which also block
    // sent operations (publish). because we have only one io thread now
    // so increase sync timeout to 10s, which is more than numMessages * sleepTimeInConsume
    Hedwig::Configuration* conf = new TestServerConfiguration(syncTimeout);
    std::auto_ptr<Hedwig::Configuration> confptr(conf);

    Hedwig::Client* client = new Hedwig::Client(*conf);
    std::auto_ptr<Hedwig::Client> clientptr(client);

    Hedwig::Subscriber& sub = client->getSubscriber();
    Hedwig::Publisher& pub = client->getPublisher();

    sub.subscribe(topic, sid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
    
    // we don't start delivery first, so the message will be queued
    // publish ${numMessages} messages, so the messages will be queued
    for (int i=1; i<=numMessages; i++) {
      std::stringstream ss;
      ss << i;
      pub.publish(topic, ss.str()); 
    }

    MyOrderCheckingMessageHandlerCallback* cb = new MyOrderCheckingMessageHandlerCallback(topic, sid, 0, sleepTimeInConsume);
    Hedwig::MessageHandlerCallbackPtr handler(cb);

    // create a thread to publish another ${numMessages} messages
    boost::thread pubThread(IntegerPublisher(topic, numMessages, numMessages, sleepTimeInConsume, pub, 0));

    // start delivery will consumed the queued messages
    // new message will recevied and the queued message should be consumed
    // hedwig should ensure the message are received in order
    sub.startDelivery(topic, sid, handler);

    // wait until message are all published
    pubThread.join();

    for (int i = 0; i < 10; i++) {
      sleep(3);
      if (cb->numMessagesReceived() == 2 * numMessages) {
        break;
      }
    }
    CPPUNIT_ASSERT(cb->inOrder());
  }

  // check message ordering
  void testPubSubInMultiDispatchThreads() {
    std::string topic = "PubSubInMultiDispatchThreadsTopic-";
    std::string sid = "mysub-0";

    int syncTimeout = 10000;
    int numDispatchThreads = 4;
    int numMessages = 100;
    int numTopics = 20;

    Hedwig::Configuration* conf = new TestServerConfiguration(syncTimeout, numDispatchThreads);
    std::auto_ptr<Hedwig::Configuration> confptr(conf);

    Hedwig::Client* client = new Hedwig::Client(*conf);
    std::auto_ptr<Hedwig::Client> clientptr(client);

    Hedwig::Subscriber& sub = client->getSubscriber();
    Hedwig::Publisher& pub = client->getPublisher();

    std::vector<Hedwig::MessageHandlerCallbackPtr> callbacks;

    for (int i=0; i<numTopics; i++) {
      std::stringstream ss;
      ss << topic << i;
      sub.subscribe(ss.str(), sid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);

      MyOrderCheckingMessageHandlerCallback* cb = new MyOrderCheckingMessageHandlerCallback(ss.str(), sid, 0, 0);
      Hedwig::MessageHandlerCallbackPtr handler(cb);
      sub.startDelivery(ss.str(), sid, handler);
      callbacks.push_back(handler);
    }

    std::vector<boost::shared_ptr<boost::thread> > threads;

    for (int i=0; i<numTopics; i++) {
      std::stringstream ss;
      ss << topic << i;
      boost::shared_ptr<boost::thread> t = boost::shared_ptr<boost::thread>(
        new boost::thread(IntegerPublisher(ss.str(), 0, numMessages, 0, pub, 0)));
      threads.push_back(t);
    }

    for (int i=0; i<numTopics; i++) {
      threads[i]->join();
    }
    threads.clear();

    for (int j=0; j<numTopics; j++) {
      MyOrderCheckingMessageHandlerCallback *cb =
        (MyOrderCheckingMessageHandlerCallback *)(callbacks[j].get());
      for (int i = 0; i < 10; i++) {
        if (cb->numMessagesReceived() == numMessages) {
          break;
        }
        sleep(3);
      }
      CPPUNIT_ASSERT(cb->inOrder());
    }
    callbacks.clear();
  }


  void testPubSubContinuousOverClose() {
    std::string topic = "pubSubTopic";
    std::string sid = "MySubscriberid-1";

    Hedwig::Configuration* conf = new TestServerConfiguration();
    std::auto_ptr<Hedwig::Configuration> confptr(conf);
    
    Hedwig::Client* client = new Hedwig::Client(*conf);
    std::auto_ptr<Hedwig::Client> clientptr(client);

    Hedwig::Subscriber& sub = client->getSubscriber();
    Hedwig::Publisher& pub = client->getPublisher();

    sub.subscribe(topic, sid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
    MyMessageHandlerCallback* cb = new MyMessageHandlerCallback(topic, sid);
    Hedwig::MessageHandlerCallbackPtr handler(cb);

    sub.startDelivery(topic, sid, handler);
    pub.publish(topic, "Test Message 1");
    bool pass = false;
    for (int i = 0; i < 10; i++) {
      sleep(3);
      if (cb->numMessagesReceived() > 0) {
	if (cb->getLastMessage() == "Test Message 1") {
	  pass = true;
	  break;
	}
      }
    }
    CPPUNIT_ASSERT(pass);
    sub.closeSubscription(topic, sid);

    pub.publish(topic, "Test Message 2");
    
    sub.subscribe(topic, sid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
    sub.startDelivery(topic, sid, handler);
    pass = false;
    for (int i = 0; i < 10; i++) {
      sleep(3);
      if (cb->numMessagesReceived() > 0) {
	if (cb->getLastMessage() == "Test Message 2") {
	  pass = true;
	  break;
	}
      }
    }
    CPPUNIT_ASSERT(pass);
  }


  /*  void testPubSubContinuousOverServerDown() {
    std::string topic = "pubSubTopic";
    std::string sid = "MySubscriberid-1";

    Hedwig::Configuration* conf = new TestServerConfiguration();
    std::auto_ptr<Hedwig::Configuration> confptr(conf);
    
    Hedwig::Client* client = new Hedwig::Client(*conf);
    std::auto_ptr<Hedwig::Client> clientptr(client);

    Hedwig::Subscriber& sub = client->getSubscriber();
    Hedwig::Publisher& pub = client->getPublisher();

    sub.subscribe(topic, sid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
    MyMessageHandlerCallback* cb = new MyMessageHandlerCallback(topic, sid);
    Hedwig::MessageHandlerCallbackPtr handler(cb);

    sub.startDelivery(topic, sid, handler);
    pub.publish(topic, "Test Message 1");
    bool pass = false;
    for (int i = 0; i < 10; i++) {
      sleep(3);
      if (cb->numMessagesReceived() > 0) {
	if (cb->getLastMessage() == "Test Message 1") {
	  pass = true;
	  break;
	}
      }
    }
    CPPUNIT_ASSERT(pass);
    sub.closeSubscription(topic, sid);

    pub.publish(topic, "Test Message 2");
    
    sub.subscribe(topic, sid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
    sub.startDelivery(topic, sid, handler);
    pass = false;
    for (int i = 0; i < 10; i++) {
      sleep(3);
      if (cb->numMessagesReceived() > 0) {
	if (cb->getLastMessage() == "Test Message 2") {
	  pass = true;
	  break;
	}
      }
    }
    CPPUNIT_ASSERT(pass);
    }*/

  void testMultiTopic() {
    std::string topicA = "pubSubTopicA";
    std::string topicB = "pubSubTopicB";
    std::string sid = "MySubscriberid-3";

    Hedwig::Configuration* conf = new TestServerConfiguration();
    std::auto_ptr<Hedwig::Configuration> confptr(conf);
    
    Hedwig::Client* client = new Hedwig::Client(*conf);
    std::auto_ptr<Hedwig::Client> clientptr(client);

    Hedwig::Subscriber& sub = client->getSubscriber();
    Hedwig::Publisher& pub = client->getPublisher();

    sub.subscribe(topicA, sid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
    sub.subscribe(topicB, sid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
   
    MyMessageHandlerCallback* cbA = new MyMessageHandlerCallback(topicA, sid);
    Hedwig::MessageHandlerCallbackPtr handlerA(cbA);
    sub.startDelivery(topicA, sid, handlerA);

    MyMessageHandlerCallback* cbB = new MyMessageHandlerCallback(topicB, sid);
    Hedwig::MessageHandlerCallbackPtr handlerB(cbB);
    sub.startDelivery(topicB, sid, handlerB);

    pub.publish(topicA, "Test Message A");
    pub.publish(topicB, "Test Message B");
    int passA = false, passB = false;
    
    for (int i = 0; i < 10; i++) {
      sleep(3);
      if (cbA->numMessagesReceived() > 0) {
	if (cbA->getLastMessage() == "Test Message A") {
	  passA = true;
	}
      }
      if (cbB->numMessagesReceived() > 0) {
	if (cbB->getLastMessage() == "Test Message B") {
	  passB = true;
	}
      }
      if (passA && passB) {
	break;
      }
    }
    CPPUNIT_ASSERT(passA && passB);
  }

  void testMultiTopicMultiSubscriber() {
    std::string topicA = "pubSubTopicA";
    std::string topicB = "pubSubTopicB";
    std::string sidA = "MySubscriberid-4";
    std::string sidB = "MySubscriberid-5";

    Hedwig::Configuration* conf = new TestServerConfiguration();
    std::auto_ptr<Hedwig::Configuration> confptr(conf);
    
    Hedwig::Client* client = new Hedwig::Client(*conf);
    std::auto_ptr<Hedwig::Client> clientptr(client);

    Hedwig::Subscriber& sub = client->getSubscriber();
    Hedwig::Publisher& pub = client->getPublisher();

    sub.subscribe(topicA, sidA, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
    sub.subscribe(topicB, sidB, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
   
    MyMessageHandlerCallback* cbA = new MyMessageHandlerCallback(topicA, sidA);
    Hedwig::MessageHandlerCallbackPtr handlerA(cbA);
    sub.startDelivery(topicA, sidA, handlerA);

    MyMessageHandlerCallback* cbB = new MyMessageHandlerCallback(topicB, sidB);
    Hedwig::MessageHandlerCallbackPtr handlerB(cbB);
    sub.startDelivery(topicB, sidB, handlerB);

    pub.publish(topicA, "Test Message A");
    pub.publish(topicB, "Test Message B");
    int passA = false, passB = false;
    
    for (int i = 0; i < 10; i++) {
      sleep(3);
      if (cbA->numMessagesReceived() > 0) {
	if (cbA->getLastMessage() == "Test Message A") {
	  passA = true;
	}
      }
      if (cbB->numMessagesReceived() > 0) {
	if (cbB->getLastMessage() == "Test Message B") {
	  passB = true;
	}
      }
      if (passA && passB) {
	break;
      }
    }
    CPPUNIT_ASSERT(passA && passB);
  }

  static const int BIG_MESSAGE_SIZE = 16436*2; // MTU to lo0 is 16436 by default on linux

  void testBigMessage() {
    std::string topic = "pubSubTopic";
    std::string sid = "MySubscriberid-6";

    Hedwig::Configuration* conf = new TestServerConfiguration();
    std::auto_ptr<Hedwig::Configuration> confptr(conf);
    
    Hedwig::Client* client = new Hedwig::Client(*conf);
    std::auto_ptr<Hedwig::Client> clientptr(client);

    Hedwig::Subscriber& sub = client->getSubscriber();
    Hedwig::Publisher& pub = client->getPublisher();

    sub.subscribe(topic, sid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
    MyMessageHandlerCallback* cb = new MyMessageHandlerCallback(topic, sid);
    Hedwig::MessageHandlerCallbackPtr handler(cb);

    sub.startDelivery(topic, sid, handler);

    char buf[BIG_MESSAGE_SIZE];
    std::string bigmessage(buf, BIG_MESSAGE_SIZE);
    pub.publish(topic, bigmessage);
    pub.publish(topic, "Test Message 1");
    bool pass = false;
    for (int i = 0; i < 10; i++) {
      sleep(3);
      if (cb->numMessagesReceived() > 0) {
	if (cb->getLastMessage() == "Test Message 1") {
	  pass = true;
	  break;
	}
      }
    }
    CPPUNIT_ASSERT(pass);
  }
};

CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( PubSubTestSuite, "PubSub" );
