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
package org.apache.hedwig.jms.jndi;

import org.apache.hedwig.jms.spi.HedwigConnectionFactoryImpl;

import javax.jms.ConnectionFactory;
import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

/**
 * Based (very very loosely) on
 * <a href="http://docs.oracle.com/javase/1.3/docs/guide/jndi/spec/spi/jndispi.fm.html">jndi guide</a>. <br/>
 * The InitialContext implementation clients should be using to get to our implementation. <br/>
 * It is possible (by configuring via administrative means for example) to use a different DirContext
 * to get to our provider implementation
 * if the various classes exposed are the same as exposed via this DirContext.<br/>
 * <p/>
 * Ideally, the env property
 * {@link javax.naming.Context.INITIAL_CONTEXT_FACTORY} "java.naming.factory.initial" is set to our factory
 * {@link HedwigInitialContextFactory} classname which will return this InitialDirContext.
 */
public class HedwigInitialContext extends InitialDirContext {

    public static final String CONNECTION_FACTORY_NAME = "jms/ConnectionFactory";
    public static final String TOPIC_CONNECTION_FACTORY_NAME = "jms/TopicConnectionFactory";
    // public static final String QUEUE_CONNECTION_FACTORY_NAME = "jms/QueueConnectionFactory";

    // Hardcoding to point to HedwigConnectionFactoryImpl by default.
    private static final Set<String> defaultNamesMapping;
    static {
        Set<String> set = new HashSet<String>(8);

        // The actual name's for the various factories are bound by an admin. For convinence sake,
        // we are providing default bindings.

        // The default connection
        set.add("jms/ConnectionFactory");
        set.add("jms/TopicConnectionFactory");
        // Add in future - for now, we do not support it.
        // set.add("jms/QueueConnectionFactory");


        set.add("ConnectionFactory");
        set.add("TopicConnectionFactory");
        // Add in future - for now, we do not support it.
        // set.add("QueueConnectionFactory");
        defaultNamesMapping = Collections.unmodifiableSet(set);
    }

    protected HedwigInitialContext(boolean lazy) throws NamingException {
        super(lazy);
    }

    public HedwigInitialContext() throws NamingException {
        super();
    }

    public HedwigInitialContext(Hashtable<?, ?> environment) throws NamingException {
        super(environment);
    }

    private ConnectionFactory ourLookup(String name){
        if (defaultNamesMapping.contains(name)){
            return new HedwigConnectionFactoryImpl();
        }

        return null;
    }

    @Override
    public Object lookup(String name) throws NamingException {
        ConnectionFactory factory = ourLookup(name);
        if (null != factory) return factory;

        return super.lookup(name);
    }

    @Override
    public Object lookup(Name name) throws NamingException {
        ConnectionFactory factory = ourLookup(name.toString());
        if (null != factory) return factory;

        return super.lookup(name);
    }
}