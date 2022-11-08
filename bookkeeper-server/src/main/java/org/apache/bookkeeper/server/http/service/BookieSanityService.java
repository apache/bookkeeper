/*
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
 */
package org.apache.bookkeeper.server.http.service;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.bookkeeper.bookie.StateManager;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.tools.cli.commands.bookie.SanityTestCommand;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * HttpEndpointService that exposes the bookie sanity state.
 *
 * <p>Get the current bookie sanity response:
 *
 * <pre>
 * <code>
 * {
 *  "passed" : true,
 *  "readOnly" : false
 *}
 * </code>
 * </pre>
 */
public class BookieSanityService implements HttpEndpointService {

    private final ServerConfiguration config;

    public BookieSanityService(ServerConfiguration config) {
        this.config = checkNotNull(config);
    }

    /**
     * POJO definition for the bookie sanity response.
     */
    @Data
    @NoArgsConstructor
    public static class BookieSanity {
        private boolean passed;
        private boolean readOnly;
    }

    @Override
	public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
		HttpServiceResponse response = new HttpServiceResponse();

		if (HttpServer.Method.GET != request.getMethod()) {
			response.setCode(HttpServer.StatusCode.NOT_FOUND);
			response.setBody("Only support GET method to retrieve bookie sanity state.");
			return response;
		}

		BookieSanity bs = new BookieSanity();
		if (config.isForceReadOnlyBookie()) {
			bs.readOnly = true;
		} else {
			SanityTestCommand sanity = new SanityTestCommand();
			bs.passed = sanity.apply(config, new SanityTestCommand.SanityFlags());
		}

		String jsonResponse = JsonUtil.toJson(bs);
		response.setBody(jsonResponse);
		response.setCode(HttpServer.StatusCode.OK);
		return response;
	}
}
