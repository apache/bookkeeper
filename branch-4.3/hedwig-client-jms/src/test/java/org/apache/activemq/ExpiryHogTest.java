/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq;

import java.util.concurrent.TimeUnit;
import javax.jms.ConnectionFactory;
import javax.jms.Session;
import javax.jms.TextMessage;





/**
 * User: gtully
 */
public class ExpiryHogTest extends JmsMultipleClientsTestSupport {
    boolean sleep = false;

    int numMessages = 4;

    public void testImmediateDispatchWhenCacheDisabled() throws Exception {
        ConnectionFactory f = createConnectionFactory();
        destination = createDestination();
        startConsumers(f, destination);
        sleep = true;
        this.startProducers(f, destination, numMessages);
        allMessagesList.assertMessagesReceived(numMessages);
    }

    protected TextMessage createTextMessage(Session session, String initText) throws Exception {
        if (sleep) {
            TimeUnit.SECONDS.sleep(10);
        }
        TextMessage msg = super.createTextMessage(session, initText);
        // what is the point of setting this !
        // msg.setJMSExpiration(4000);
        return msg;
    }

    @Override
    protected void setUp() throws Exception {
        autoFail = false;
        persistent = true;
        super.setUp();
    }
}
