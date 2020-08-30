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

import java.io.IOException;
import java.io.Writer;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.bookkeeper.http.AbstractHttpHandlerFactory;
import org.apache.bookkeeper.http.HttpRouter;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.HttpServer.ApiType;
import org.apache.bookkeeper.http.HttpServiceProvider;
import org.apache.bookkeeper.http.service.ErrorHttpService;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bookie http service servlet.
 *
 **/
public class BookieHttpServiceServlet extends HttpServlet {
  static final Logger LOG = LoggerFactory.getLogger(BookieHttpServiceServlet.class);

  // url to api
  private final Map<String/*url*/, ApiType/*api*/> mappings = new ConcurrentHashMap<>();

  public BookieHttpServiceServlet(){
    HttpRouter<ApiType> router = new HttpRouter<HttpServer.ApiType>(
      new AbstractHttpHandlerFactory<ApiType>(BookieServletHttpServer.getBookieHttpServiceProvider()) {
        @Override
        public HttpServer.ApiType newHandler(HttpServer.ApiType apiType) {
          return apiType;
        }
      }) {
      @Override
      public void bindHandler(String endpoint, HttpServer.ApiType mapping) {
        mappings.put(endpoint, mapping);
      }
    };
    router.bindAll();
  }

  @Override
  protected void service(HttpServletRequest httpRequest, HttpServletResponse httpResponse) throws IOException {
    HttpServiceRequest request = new HttpServiceRequest()
                                .setMethod(httpServerMethod(httpRequest))
                                .setParams(httpServletParams(httpRequest))
                                .setBody(IOUtils.toString(httpRequest.getInputStream(), "UTF-8"));
    String uri = httpRequest.getRequestURI();
    HttpServiceResponse response;
    try {
      HttpServer.ApiType apiType = mappings.get(uri);
      HttpServiceProvider bookie = BookieServletHttpServer.getBookieHttpServiceProvider();
      if (bookie == null) {
        httpResponse.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        return;
      }
      HttpEndpointService httpEndpointService = bookie.provideHttpEndpointService(apiType);
      if (httpEndpointService == null) {
        httpResponse.sendError(HttpServletResponse.SC_NOT_FOUND);
        return;
      }
      response = httpEndpointService.handle(request);
    } catch (Throwable e) {
      LOG.error("Error while service Bookie API request " + uri, e);
      response = new ErrorHttpService().handle(request);
    }
    if (response != null) {
      httpResponse.setStatus(response.getStatusCode());
      try (Writer out = httpResponse.getWriter()) {
        out.write(response.getBody());
      }
    } else {
      httpResponse.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }


  /**
   * Convert http request parameters to a map.
   */
  @SuppressWarnings("unchecked")
  Map<String, String> httpServletParams(HttpServletRequest request) {
    Map<String, String> map = new HashMap<>();
    for (Enumeration<String> param = request.getParameterNames();
         param.hasMoreElements();) {
      String pName = param.nextElement();
      map.put(pName, request.getParameter(pName));
    }
    return map;
  }

  /**
   * Get servlet request method and convert to the method that can be recognized by HttpServer.
   */
  HttpServer.Method httpServerMethod(HttpServletRequest request) {
    switch (request.getMethod()) {
      case "POST":
        return HttpServer.Method.POST;
      case "DELETE":
        return HttpServer.Method.DELETE;
      case "PUT":
        return HttpServer.Method.PUT;
      case "GET":
        return HttpServer.Method.GET;
      default:
        throw new UnsupportedOperationException("Unsupported http method");
    }
  }
}
