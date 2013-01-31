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
package org.apache.hedwig.jms.spi;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import java.io.Serializable;

/**
 * Implementation of jmx ConnectionFactory
 * MUST be MT-safe (2.8)
 */
public class HedwigConnectionFactoryImpl implements ConnectionFactory, TopicConnectionFactory,
    Referenceable, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public HedwigConnectionImpl createConnection() throws JMSException {
        HedwigConnectionImpl retval = new HedwigConnectionImpl();
        return retval;
    }

    @Override
    public HedwigConnectionImpl createConnection(String user, String password) throws JMSException {
        HedwigConnectionImpl retval = new HedwigConnectionImpl(user, password);
        return retval;
    }

    @Override
    public Reference getReference() throws NamingException {
        return new Reference(getClass().getName());
    }

    @Override
    public TopicConnection createTopicConnection() throws JMSException {
        return new HedwigConnectionImpl();
    }

    @Override
    public TopicConnection createTopicConnection(String user, String password) throws JMSException {
        return new HedwigConnectionImpl(user, password);
    }
}
