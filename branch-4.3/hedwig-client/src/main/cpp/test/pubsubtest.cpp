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

#include "gtest/gtest.h"
#include <boost/thread/mutex.hpp>

#include "../lib/clientimpl.h"
#include <hedwig/exceptions.h>
#include <hedwig/callback.h>
#include <stdexcept>
#include <pthread.h>

#include <log4cxx/logger.h>

#include "util.h"

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("hedwig."__FILE__));

class StartStopDeliveryMsgHandler : public Hedwig::MessageHandlerCallback {
public:
  StartStopDeliveryMsgHandler(Hedwig::Subscriber& subscriber, const int nextValue)
    : subscriber(subscriber), nextValue(nextValue) {}

  virtual void consume(const std::string& topic, const std::string& subscriberId,
                       const Hedwig::Message& msg,
                       Hedwig::OperationCallbackPtr& callback) {
    {
      boost::lock_guard<boost::mutex> lock(mutex);

      int curVal = atoi(msg.body().c_str());
      LOG4CXX_DEBUG(logger, "received message " << curVal);
      if (curVal == nextValue) {
        ++nextValue;
      }
      callback->operationComplete();
    }
    ASSERT_THROW(subscriber.startDelivery(topic, subscriberId,
                                          Hedwig::MessageHandlerCallbackPtr()),
                 Hedwig::StartingDeliveryException);
    ASSERT_THROW(subscriber.stopDelivery(topic, subscriberId),
                 Hedwig::StartingDeliveryException);
  }

  int getNextValue() {
    return nextValue;
  }

private:
  Hedwig::Subscriber& subscriber;
  boost::mutex mutex;
  int nextValue;
};

class PubSubMessageHandlerCallback : public Hedwig::MessageHandlerCallback {
public:
  PubSubMessageHandlerCallback(const std::string& topic, const std::string& subscriberId) : messagesReceived(0), topic(topic), subscriberId(subscriberId) {
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
class PubSubOrderCheckingMessageHandlerCallback : public Hedwig::MessageHandlerCallback {
public:
  PubSubOrderCheckingMessageHandlerCallback(const std::string& topic, const std::string& subscriberId, const int startMsgId, const int sleepTimeInConsume)
    : topic(topic), subscriberId(subscriberId), startMsgId(startMsgId),
      nextMsgId(startMsgId), isInOrder(true), sleepTimeInConsume(sleepTimeInConsume) {
  }

  virtual void consume(const std::string& topic, const std::string& subscriberId,
		       const Hedwig::Message& msg, Hedwig::OperationCallbackPtr& callback) {
    if (topic == this->topic && subscriberId == this->subscriberId) {
      boost::lock_guard<boost::mutex> lock(mutex);

      int newMsgId = atoi(msg.body().c_str());
      if (newMsgId == nextMsgId + 1) {
        // only calculate unduplicated entries
        ++nextMsgId;
      }

      // checking msgId
      LOG4CXX_DEBUG(logger, "received message " << newMsgId);
      if (startMsgId >= 0) { // need to check ordering if start msg id is larger than 0
	if (isInOrder) {
          // in some environments, ssl channel encountering error like Bad File Descriptor.
          // the channel would disconnect and reconnect. A duplicated message would be received.
          // so just checking we received a larger out-of-order message.
	  if (newMsgId > startMsgId + 1) {
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
    
  int nextExpectedMsgId() {
    boost::lock_guard<boost::mutex> lock(mutex);
    return nextMsgId;
  }    

  bool inOrder() {
    boost::lock_guard<boost::mutex> lock(mutex);
    return isInOrder;
  }
    
protected:
  boost::mutex mutex;
  std::string topic;
  std::string subscriberId;
  int startMsgId;
  int nextMsgId;
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

TEST(PubSubTest, testStartDeliveryWithoutSub) {
  Hedwig::Configuration* conf = new TestServerConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);
  
  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();

  std::string topic = "testStartDeliveryWithoutSub";
  std::string sid = "mysub";

  PubSubMessageHandlerCallback* cb = new PubSubMessageHandlerCallback(topic, sid);
  Hedwig::MessageHandlerCallbackPtr handler(cb);
  ASSERT_THROW(sub.startDelivery(topic, sid, handler),
               Hedwig::NotSubscribedException);
}

TEST(PubSubTest, testAlreadyStartDelivery) {
  Hedwig::Configuration* conf = new TestServerConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);
  
  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();

  std::string topic = "testAlreadyStartDelivery";
  std::string sid = "mysub";

  sub.subscribe(topic, sid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);

  PubSubMessageHandlerCallback* cb = new PubSubMessageHandlerCallback(topic, sid);
  Hedwig::MessageHandlerCallbackPtr handler(cb);
  sub.startDelivery(topic, sid, handler);
  ASSERT_THROW(sub.startDelivery(topic, sid, handler),
               Hedwig::AlreadyStartDeliveryException);
}

TEST(PubSubTest, testStopDeliveryWithoutSub) {
  Hedwig::Configuration* conf = new TestServerConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);
  
  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
  ASSERT_THROW(sub.stopDelivery("testStopDeliveryWithoutSub", "mysub"),
               Hedwig::NotSubscribedException);
}

TEST(PubSubTest, testStopDeliveryTwice) {
  Hedwig::Configuration* conf = new TestServerConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);
  
  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();

  std::string topic = "testStopDeliveryTwice";
  std::string subid = "mysub";

  sub.subscribe(topic, subid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);

  // it is ok to stop delivery without start delivery
  sub.stopDelivery(topic, subid);

  PubSubMessageHandlerCallback* cb = new PubSubMessageHandlerCallback(topic, subid);
  Hedwig::MessageHandlerCallbackPtr handler(cb);
  sub.startDelivery(topic, subid, handler);
  sub.stopDelivery(topic, subid);
  // stop again
  sub.stopDelivery(topic, subid);
}

// test startDelivery / stopDelivery in msg handler
TEST(PubSubTest, testStartStopDeliveryInMsgHandler) {
  std::string topic("startStopDeliveryInMsgHandler");
  std::string subscriber("mysubid");

  Hedwig::Configuration* conf = new TestServerConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);

  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
  Hedwig::Publisher& pub = client->getPublisher();

  // subscribe topic
  sub.subscribe(topic, subscriber, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);

  int numMsgs = 5;

  for (int i=0; i<numMsgs; i++) {
    std::stringstream oss;
    oss << i;
    pub.publish(topic, oss.str());
  }

  // sleep for a while to wait all messages are sent to subscribe and queue them
  sleep(1);

  StartStopDeliveryMsgHandler* cb = new StartStopDeliveryMsgHandler(sub, 0);
  Hedwig::MessageHandlerCallbackPtr handler(cb);
  sub.startDelivery(topic, subscriber, handler);

  for (int i=0 ; i<10; i++) {
    if (cb->getNextValue() == numMsgs) {
      break;
    } else {
      sleep(1);
    }
  }
  ASSERT_TRUE(cb->getNextValue() == numMsgs);

  sub.stopDelivery(topic, subscriber);
  sub.closeSubscription(topic, subscriber);
}

// test startDelivery / stopDelivery randomly
TEST(PubSubTest, testRandomDelivery) {
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
   PubSubOrderCheckingMessageHandlerCallback* cb =
     new PubSubOrderCheckingMessageHandlerCallback(topic, subscriber, 0, 0);
   Hedwig::MessageHandlerCallbackPtr handler(cb);

   for (int i = 0; i < nLoops; i++) {
     LOG4CXX_DEBUG(logger, "Randomly Delivery : " << i);
     sub.startDelivery(topic, subscriber, handler);
     // sleep random time
     usleep(rand()%1000000);
     sub.stopDelivery(topic, subscriber);
     ASSERT_TRUE(cb->inOrder());
   }

   pubThread.join();
 }

 // check message ordering
 TEST(PubSubTest, testPubSubOrderChecking) {
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

   PubSubOrderCheckingMessageHandlerCallback* cb = new PubSubOrderCheckingMessageHandlerCallback(topic, sid, 0, sleepTimeInConsume);
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
     if (cb->nextExpectedMsgId() == 2 * numMessages) {
       break;
     }
   }
   ASSERT_TRUE(cb->inOrder());
 }

 // check message ordering
 TEST(PubSubTest, testPubSubInMultiDispatchThreads) {
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

     PubSubOrderCheckingMessageHandlerCallback* cb = new PubSubOrderCheckingMessageHandlerCallback(ss.str(), sid, 0, 0);
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
     PubSubOrderCheckingMessageHandlerCallback *cb =
       (PubSubOrderCheckingMessageHandlerCallback *)(callbacks[j].get());
     for (int i = 0; i < 10; i++) {
       if (cb->nextExpectedMsgId() == numMessages) {
	 break;
       }
       sleep(3);
     }
     ASSERT_TRUE(cb->inOrder());
   }
   callbacks.clear();
 }

 TEST(PubSubTest, testPubSubContinuousOverClose) {
   std::string topic = "pubSubTopic";
   std::string sid = "MySubscriberid-1";

   Hedwig::Configuration* conf = new TestServerConfiguration();
   std::auto_ptr<Hedwig::Configuration> confptr(conf);

   Hedwig::Client* client = new Hedwig::Client(*conf);
   std::auto_ptr<Hedwig::Client> clientptr(client);

   Hedwig::Subscriber& sub = client->getSubscriber();
   Hedwig::Publisher& pub = client->getPublisher();

   sub.subscribe(topic, sid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
   PubSubMessageHandlerCallback* cb = new PubSubMessageHandlerCallback(topic, sid);
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
   ASSERT_TRUE(pass);
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
   ASSERT_TRUE(pass);
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
     PubSubMessageHandlerCallback* cb = new PubSubMessageHandlerCallback(topic, sid);
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

 TEST(PubSubTest, testMultiTopic) {
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
   
  PubSubMessageHandlerCallback* cbA = new PubSubMessageHandlerCallback(topicA, sid);
  Hedwig::MessageHandlerCallbackPtr handlerA(cbA);
  sub.startDelivery(topicA, sid, handlerA);

  PubSubMessageHandlerCallback* cbB = new PubSubMessageHandlerCallback(topicB, sid);
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
  ASSERT_TRUE(passA && passB);
}

TEST(PubSubTest, testMultiTopicMultiSubscriber) {
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
   
  PubSubMessageHandlerCallback* cbA = new PubSubMessageHandlerCallback(topicA, sidA);
  Hedwig::MessageHandlerCallbackPtr handlerA(cbA);
  sub.startDelivery(topicA, sidA, handlerA);

  PubSubMessageHandlerCallback* cbB = new PubSubMessageHandlerCallback(topicB, sidB);
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
  ASSERT_TRUE(passA && passB);
}

static const int BIG_MESSAGE_SIZE = 16436*2; // MTU to lo0 is 16436 by default on linux

TEST(PubSubTest, testBigMessage) {
  std::string topic = "pubSubTopic";
  std::string sid = "MySubscriberid-6";

  Hedwig::Configuration* conf = new TestServerConfiguration();
  std::auto_ptr<Hedwig::Configuration> confptr(conf);
    
  Hedwig::Client* client = new Hedwig::Client(*conf);
  std::auto_ptr<Hedwig::Client> clientptr(client);

  Hedwig::Subscriber& sub = client->getSubscriber();
  Hedwig::Publisher& pub = client->getPublisher();

  sub.subscribe(topic, sid, Hedwig::SubscribeRequest::CREATE_OR_ATTACH);
  PubSubMessageHandlerCallback* cb = new PubSubMessageHandlerCallback(topic, sid);
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
  ASSERT_TRUE(pass);
}
