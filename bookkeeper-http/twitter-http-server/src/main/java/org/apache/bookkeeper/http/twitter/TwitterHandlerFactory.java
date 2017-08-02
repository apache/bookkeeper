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
package org.apache.bookkeeper.http.twitter;

import com.twitter.finagle.http.Request;
import com.twitter.finagle.http.Response;
import com.twitter.util.Future;

import org.apache.bookkeeper.http.AbstractHandlerFactory;
import org.apache.bookkeeper.http.ServiceProvider;



/**
 * Factory which provide http handlers for TwitterServer based Http Server.
 */
public class TwitterHandlerFactory extends AbstractHandlerFactory<TwitterAbstractHandler> {

    public TwitterHandlerFactory(ServiceProvider serviceProvider) {
        super(serviceProvider);
    }

    @Override
    public TwitterAbstractHandler newHeartbeatHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getServiceProvider().provideHeartbeatService(), request);
            }
        };
    }

    public TwitterAbstractHandler newConfigurationHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getServiceProvider().provideConfigurationService(), request);
            }
        };
    }

}
