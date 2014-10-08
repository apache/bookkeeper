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
package org.apache.hedwig.jms;

import junit.framework.Assert;
import org.apache.hedwig.JmsTestBase;
import org.apache.hedwig.jms.jndi.HedwigInitialContext;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.Context;
import javax.naming.NamingException;

/**
 * Basic JMS testcases.
 */
public class BasicJMSTest extends JmsTestBase {

    private static final int NUM_ITERATIONS = 1;
    private static final String TEXT_MESSAGE = "test_message";

    // private static final String CHAT_TOPIC_NAME = "chat_topic";
    private static final String[] CHAT_MESSAGES = {"message1", "message2", "message3", "message4"};
    private static final String ATTRIBUTE_KEY = "key";
    private static final String ATTRIBUTE_VALUE = "value";

    private TopicConnectionFactory topicConnectionFactory;

    // private static final String testTopicName = "test_topic3";
    private String testTopicName;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        Context messaging = new HedwigInitialContext();
        topicConnectionFactory = (TopicConnectionFactory) messaging.lookup(
                HedwigInitialContext.TOPIC_CONNECTION_FACTORY_NAME);
        testTopicName = SessionImpl.generateRandomString();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        testTopicName = null;
    }

    public void testSimpleJms() throws JMSException {
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            simpleJMSTestImpl(false);
        }
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            simpleJMSTestImpl(true);
        }
    }

    private void simpleJMSTestImpl(boolean transacted) throws JMSException {
        TopicConnection connection = topicConnectionFactory.createTopicConnection();

        // Creating two sessions : one to subscribe, the other to publish and test between them ...
        TopicSession publishTopicSession = connection.createTopicSession(transacted, Session.AUTO_ACKNOWLEDGE);
        TopicSession subscribeTopicSession = connection.createTopicSession(transacted, Session.AUTO_ACKNOWLEDGE);

        TopicPublisher publisher = publishTopicSession.createPublisher(publishTopicSession.createTopic(testTopicName));
        TopicSubscriber subscriber = subscribeTopicSession.createDurableSubscriber(
                publishTopicSession.createTopic(testTopicName), "test_subscriber");
        //TopicSubscriber subscriber_dup =
        subscribeTopicSession.createDurableSubscriber(
                publishTopicSession.createTopic(testTopicName), "test_subscriber");
        // TopicSubscriber subscriber1 =
        subscribeTopicSession.createDurableSubscriber(
                publishTopicSession.createTopic(testTopicName), "test_subscriber1");

        // Start connection ...
        connection.start();
        // subscriber.receiveNoWait();

        publisher.publish(publishTopicSession.createTextMessage(TEXT_MESSAGE));
        if (transacted) publishTopicSession.commit();

        Message message = subscriber.receive();

        Assert.assertNotNull(message);
        Assert.assertTrue(message instanceof TextMessage);
        Assert.assertEquals(((TextMessage) message).getText(), TEXT_MESSAGE);

        if (transacted) subscribeTopicSession.commit();

        subscribeTopicSession.close();
        // Must return null, we have closed the session.
        Assert.assertNull(subscriber.receive());

        connection.close();
    }


    // Based on code from http://onjava.com/pub/a/onjava/excerpt/jms_ch2/index.html?page=2
    public void testSimpleChat() throws NamingException, JMSException {
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            simpleChatTestImpl(false);
        }
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            simpleChatTestImpl(true);
        }
    }

    private void simpleChatTestImpl(boolean transacted) throws NamingException, JMSException {
        // Create a JMS connection
        TopicConnection connection = topicConnectionFactory.createTopicConnection();

        // Create two JMS session objects
        TopicSession pubSession = connection.createTopicSession(transacted, Session.AUTO_ACKNOWLEDGE);
        TopicSession subSession = connection.createTopicSession(transacted, Session.AUTO_ACKNOWLEDGE);

        // Look up a JMS topic
        // Topic chatTopic = pubSession.createTopic(CHAT_TOPIC_NAME);
        Topic chatTopic = pubSession.createTopic(SessionImpl.generateRandomString());

        // Create a JMS publisher and subscriber
        TopicPublisher publisher = pubSession.createPublisher(chatTopic);
        TopicSubscriber subscriber = subSession.createSubscriber(chatTopic);
        TopicSubscriber subscriber1 = subSession.createSubscriber(chatTopic);

        final Mutable<Boolean> error = new Mutable<Boolean>(false);
        final Mutable<String> errorMessage = new Mutable<String>(null);
        final Mutable<Integer> messageCount = new Mutable<Integer>(0);
        // Set a JMS message listener
        subscriber.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                // if already failed, ignore.
                if (error.getValue()) return;

                if (!(message instanceof TextMessage)) {
                    errorMessage.setValue("Not text message ?");
                    error.setValue(true);
                    return;
                }
                TextMessage textMessage = (TextMessage) message;
                String text;
                try {
                    text = textMessage.getText();
                } catch (JMSException e) {
                    e.printStackTrace();
                    errorMessage.setValue("Exception getting text : " + e);
                    error.setValue(true);
                    return;
                }

                final int count = messageCount.getValue();
                messageCount.setValue(messageCount.getValue() + 1);

                if (count >= CHAT_MESSAGES.length) {
                    errorMessage.setValue("Unexpected message count : " + count);
                    error.setValue(true);
                    return;
                }
                if (!CHAT_MESSAGES[count].equals(text)) {
                    errorMessage.setValue("Message mismatch. expected : "
                                          + CHAT_MESSAGES[count] + ", received : " + text);
                    error.setValue(true);
                    return;
                }
                try {
                    if (!ATTRIBUTE_VALUE.equals(textMessage.getStringProperty(ATTRIBUTE_KEY))) {
                        errorMessage.setValue("Attribute value mismatch. Expected : " + ATTRIBUTE_VALUE
                                              + ", found : " + textMessage.getStringProperty(ATTRIBUTE_KEY));
                        error.setValue(true);
                        return;
                    }
                } catch (JMSException e) {
                    e.printStackTrace();
                    errorMessage.setValue("Exception getting text : " + e);
                    error.setValue(true);
                    return;
                }
            }
        });

        // Start the JMS connection; allows messages to be delivered
        connection.start();
        for (String message : CHAT_MESSAGES) {
            TextMessage tmessage = pubSession.createTextMessage(message);
            tmessage.setStringProperty(ATTRIBUTE_KEY, ATTRIBUTE_VALUE);
            publisher.publish(tmessage);
        }

        if (transacted) pubSession.commit();

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < CHAT_MESSAGES.length; i++) {
            Message receivedMessage = subscriber1.receive(100);
            Assert.assertNotNull(receivedMessage);
        }

        if (messageCount.getValue() != CHAT_MESSAGES.length) {
            error.setValue(true);
            errorMessage.setValue("Expected to receive " + CHAT_MESSAGES.length
                                  + ", got " + messageCount.getValue() + " messages.");
        }

        if (transacted) subSession.commit();

        Assert.assertFalse(String.valueOf(error.getValue()), error.getValue());
        connection.close();
    }


    public void testSimpleSelector() throws JMSException {
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            simpleSelectorImpl(false);
        }
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            simpleSelectorImpl(true);
        }
    }

    private void simpleSelectorImpl(boolean transacted) throws JMSException {
        TopicConnection connection = topicConnectionFactory.createTopicConnection();

        // Creating three sessions : one to subscribe with selector,
        // one to publish and third to validate that the message was/was-not delivered !
        TopicSession publishTopicSession = connection.createTopicSession(transacted, Session.AUTO_ACKNOWLEDGE);
        TopicSession subscribeTopicSession = connection.createTopicSession(transacted, Session.AUTO_ACKNOWLEDGE);
        TopicSession subscribeValidationTopicSession = connection.createTopicSession(transacted,
                                                                                     Session.AUTO_ACKNOWLEDGE);


        TopicPublisher publisher = publishTopicSession.createPublisher(publishTopicSession.createTopic(testTopicName));

        // The first subscriber's subscription should be overridden by the second
        // hence, we MUST have selector enabled.
        TopicSubscriber selectorSubscriber = subscribeTopicSession.createDurableSubscriber(
                publishTopicSession.createTopic(testTopicName), "test_subscriber");
        TopicSubscriber selectorSubscriber_dup = subscribeTopicSession.createDurableSubscriber(
                publishTopicSession.createTopic(testTopicName),
                "test_subscriber", ATTRIBUTE_KEY + " <> '" + ATTRIBUTE_VALUE + "'", false);

        // without selector.
        TopicSubscriber subscriberValidation =
                subscribeValidationTopicSession.createDurableSubscriber(
                        publishTopicSession.createTopic(testTopicName), "test_subscriber1");

        // Start connection ...
        connection.start();

        final String textMessage = TEXT_MESSAGE + ", transacted : " + transacted;
        // Send the message.
        {
            TextMessage message = publishTopicSession.createTextMessage(textMessage);
            message.setStringProperty(ATTRIBUTE_KEY, ATTRIBUTE_VALUE);
            publisher.publish(message);
        }

        if (transacted) {
            // Must return null ... no publish must happen until we commit !
            Message message = subscriberValidation.receive(200);
            Assert.assertNull("Unexpected message : " + message, message);
            publishTopicSession.commit();
        }

        // subscriberValidation must get the message as soon as it is available,
        // while selectorSubscriber might/might not
        // (depending on whether Selector works :-) ). So wait on subscriberValidation
        {
            Message receivedMessage = subscriberValidation.receive(200);

            // Validate whether it is the correct message.
            Assert.assertNotNull("receivedMessage was expected", receivedMessage);
            Assert.assertTrue("receivedMessage not a textMessage ? " + receivedMessage,
                receivedMessage instanceof TextMessage);
            Assert.assertEquals("test content does not match ? " + ((TextMessage) receivedMessage).getText(),
                textMessage, ((TextMessage) receivedMessage).getText());

            final String attrValue = receivedMessage.getStringProperty(ATTRIBUTE_KEY);
            Assert.assertEquals("attribute value invalid ? " + attrValue, attrValue, ATTRIBUTE_VALUE);

            if (transacted) subscribeValidationTopicSession.commit();
        }

        // Now that subscriberValidation received the message,
        // selectorSubscriber and/or selectorSubscriber_dup must also receive the
        // message or they will never receive the message (since selector blocked it).
        {
            // Even though selectorSubscriber was subscribed WITHOUT selector, we create selectorSubscriber_dup LATER to
            // override the subscription policy using the SAME subscription id/topic
            // - so selectorSubscriber_dup config MUST take
            // precedence ...
            Message msg = selectorSubscriber.receive(100);
            Message msg1 = selectorSubscriber_dup.receive(100);
            Assert.assertNull("Unexpected received message " + msg, msg);
            Assert.assertNull("Unexpected received message " + msg1, msg1);
        }

        // close all sessions.
        subscribeTopicSession.close();
        subscribeValidationTopicSession.close();
        publishTopicSession.close();

        // Must return null, we have closed the session ! simple validation :-)
        {
            Message smsg = subscriberValidation.receive();
            Assert.assertNull("Unexpected validation message received " + smsg, smsg);
        }

        // close connection ...
        connection.close();
    }
}
