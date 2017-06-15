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
package org.apache.bookkeeper.ssl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SecurityProviderFactoryFactory {
    private final static Logger LOG = LoggerFactory.getLogger(SecurityProviderFactoryFactory.class);

    public static SecurityHandlerFactory getSecurityProviderFactory(String securityHandler)
            throws SecurityException {
        if ((securityHandler == null) || (securityHandler.equals(""))) {
            return null;
        }

        ClassLoader classLoader = SecurityProviderFactoryFactory.class.getClassLoader();
        SecurityHandlerFactory shFactory = null;
        try {
            Class<?> shFactoryClass = classLoader.loadClass(securityHandler);
            shFactory = (SecurityHandlerFactory) shFactoryClass.newInstance();
            LOG.info("Loaded security handler for {}", securityHandler);
        } catch (Exception e) {
            LOG.error("Unable to load security handler for {}: ", securityHandler, e);
            throw new SecurityException(e);
        }
        return shFactory;
    }

}
