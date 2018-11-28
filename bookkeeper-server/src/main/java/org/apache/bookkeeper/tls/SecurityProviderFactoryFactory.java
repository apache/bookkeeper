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
package org.apache.bookkeeper.tls;

import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory to manage security provider factories.
 */
public abstract class SecurityProviderFactoryFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SecurityProviderFactoryFactory.class);

    public static SecurityHandlerFactory getSecurityProviderFactory(String securityHandler)
            throws SecurityException {
        if ((securityHandler == null) || (securityHandler.equals(""))) {
            return null;
        }

        SecurityHandlerFactory shFactory;
        try {
            Class<? extends SecurityHandlerFactory> shFactoryClass =
                ReflectionUtils.forName(securityHandler, SecurityHandlerFactory.class);
            shFactory = ReflectionUtils.newInstance(shFactoryClass);
            LOG.info("Loaded security handler for {}", securityHandler);
        } catch (RuntimeException re) {
            LOG.error("Unable to load security handler for {}: ", securityHandler, re.getCause());
            throw new SecurityException(re.getCause());
        }
        return shFactory;
    }

}
