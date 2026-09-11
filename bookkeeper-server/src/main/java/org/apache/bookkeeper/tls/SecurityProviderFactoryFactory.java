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

import lombok.CustomLog;
import org.apache.bookkeeper.common.util.ReflectionUtils;

/**
 * A factory to manage security provider factories.
 */
@CustomLog
public abstract class SecurityProviderFactoryFactory {

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
            log.info().attr("securityHandler", securityHandler).log("Loaded security handler");
        } catch (RuntimeException re) {
            log.error()
                    .attr("securityHandler", securityHandler)
                    .exception(re.getCause())
                    .log("Unable to load security handler");
            throw new SecurityException(re.getCause());
        }
        return shFactory;
    }

}
