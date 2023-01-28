/*
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
package org.apache.bookkeeper.tls;

import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.bookkeeper.auth.BookieAuthProvider;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Light weight Unit Tests for BookieAuthZFactory.
 */
public class TestBookieAuthZFactory {
    private static final Logger LOG = LoggerFactory.getLogger(TestBookieAuthZFactory.class);

    public TestBookieAuthZFactory() {
    }

    /**
     * Initialize a BookieAuthZFactory without configuring authorizedRoles in ServerConfiguration.
     * This should fail as in order to use this authorization provider, we need to have authorizedRoles set.
     */
    @Test
    public void testBookieAuthZInitNoRoles() {
        ServerConfiguration conf = new ServerConfiguration();
        String factoryClassName = BookieAuthZFactory.class.getName();
        BookieAuthProvider.Factory factory = ReflectionUtils.newInstance(factoryClassName,
                BookieAuthProvider.Factory.class);

        try {
            factory.init(conf);
            fail("Not supposed to initialize BookieAuthZFactory without authorized roles set");
        } catch (IOException | RuntimeException e) {
            LOG.info("BookieAuthZFactory did not initialize as there are no authorized roles set.");
        }
    }

    /**
     * Initialize a BookieAuthZFactory as an authProvider and configure an empty string in authorizedRoles.
     * This should fail as in order to use this as an authorization provider, we need to have valid authorizedRoles set.
     */
    @Test
    public void testBookieAuthZInitEmptyRole() {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setAuthorizedRoles("");
        String factoryClassName = BookieAuthZFactory.class.getName();
        BookieAuthProvider.Factory factory = ReflectionUtils.newInstance(factoryClassName,
                BookieAuthProvider.Factory.class);

        try {
            factory.init(conf);
            fail("Not supposed to initialize BookieAuthZFactory without authorized roles set");
        } catch (IOException | RuntimeException e) {
            LOG.info("BookieAuthZFactory did not initialize as there are no authorized roles set.");
        }
    }

    /**
     * Initialize a BookieAuthZFactory with a valid string for the configured role.
     * However, pass a null (or faulty) connection for it to authorize, it should fail.
     */
    @Test
    public void testBookieAuthZNewProviderNullAddress() {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setAuthorizedRoles("testRole");
        String factoryClassName = BookieAuthZFactory.class.getName();
        BookieAuthProvider.Factory factory = ReflectionUtils.newInstance(factoryClassName,
                BookieAuthProvider.Factory.class);

        try {
            factory.init(conf);
            BookieAuthProvider authProvider = factory.newProvider(null, null);
            authProvider.onProtocolUpgrade();
            fail("BookieAuthZFactory should fail with a null connection");
        } catch (IOException | RuntimeException e) {
        }
    }
}
