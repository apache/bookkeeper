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

import java.util.HashMap;
import java.util.Map;

/**
 * Provide the mapping of http endpoints and handlers and function
 * to bind endpoint to the corresponding handler.
 */
public abstract class HttpRouter<Handler> {

  // Define endpoints here.
  public static final String HEARTBEAT                    = "/heartbeat";
  public static final String SERVER_CONFIG                = "/api/config/serverConfig";

  private final Map<String, Handler> endpointHandlers = new HashMap<>();

  public HttpRouter(AbstractHandlerFactory<Handler> handlerFactory) {
    this.endpointHandlers.put(HEARTBEAT, handlerFactory.newHeartbeatHandler());
    this.endpointHandlers.put(SERVER_CONFIG, handlerFactory.newConfigurationHandler());
  }

  /**
   * Bind all endpoints to corresponding handlers.
   */
  public void bindAll() {
    for (Map.Entry<String, Handler> entry : endpointHandlers.entrySet()) {
      bindHandler(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Bind the given endpoint to its corresponding handlers.
   * @param endpoint http endpoint
   * @param handler http handler
   */
  public abstract void bindHandler(String endpoint, Handler handler);

}
