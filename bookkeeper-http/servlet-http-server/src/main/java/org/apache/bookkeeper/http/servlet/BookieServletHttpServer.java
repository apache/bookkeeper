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
package org.apache.bookkeeper.http.servlet;

import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.HttpServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Only use for hold Http service provider,not a fully implement bookie http service server.
 **/
public class BookieServletHttpServer implements HttpServer {
  static final Logger LOG = LoggerFactory.getLogger(BookieServletHttpServer.class);
  private static HttpServiceProvider bookieHttpServiceProvider;
  private static int listenPort = -1;

  public static HttpServiceProvider getBookieHttpServiceProvider(){
    return bookieHttpServiceProvider;
  }
  /**
   * Listen  port.
   **/
  public static int getListenPort(){
    return listenPort;
  }

  @Override
  public void initialize(HttpServiceProvider httpServiceProvider) {
     setHttpServiceProvider(httpServiceProvider);
     LOG.info("Bookie HTTP Server initialized: {}", httpServiceProvider);
  }

  public static synchronized void setHttpServiceProvider(HttpServiceProvider httpServiceProvider){
    bookieHttpServiceProvider = httpServiceProvider;
  }

  public static synchronized void setPort(int port){
    listenPort = port;
  }
  @Override
  public boolean startServer(int port) {
    setPort(port);
    return true;
  }

  @Override
  public void stopServer() {

  }

  @Override
  public boolean isRunning() {
    return true;
  }
}
