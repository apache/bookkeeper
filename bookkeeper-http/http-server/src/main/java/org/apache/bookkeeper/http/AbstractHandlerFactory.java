/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.http;

/**
 * Abstract handler factory which provide interface
 * to create handlers for bookkeeper http endpoints.
 */
public abstract class AbstractHandlerFactory<Handler> {
    private ServiceProvider serviceProvider;

    public AbstractHandlerFactory(ServiceProvider serviceProvider) {
        this.serviceProvider = serviceProvider;
    }

    public ServiceProvider getServiceProvider() {
        return serviceProvider;
    }

    /**
     * Create a handler for heartbeat api.
     */
    public abstract Handler newHeartbeatHandler();

    /**
     * Create a handler for server configuration api.
     */
    public abstract Handler newConfigurationHandler();

}
