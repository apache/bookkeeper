/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.distributedlog.stream.client.resolver;

import io.grpc.Attributes;
import io.grpc.ResolvedServerInfo;
import io.grpc.internal.SharedResourceHolder.Resource;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * A simple name resolver that returns pre-configured host addresses.
 */
class SimpleStreamResolver extends AbstractStreamResolver {

  private final List<ResolvedServerInfo> servers;

  SimpleStreamResolver(String name,
                       Resource<ExecutorService> executorResource,
                       List<URI> endpoints) {
    super(name, executorResource);
    this.servers = Collections.unmodifiableList(
      endpoints.stream()
        .map(endpoint -> new ResolvedServerInfo(
          new InetSocketAddress(endpoint.getHost(), endpoint.getPort()),
          Attributes.EMPTY))
        .collect(Collectors.toList()));
  }

  @Override
  protected List<ResolvedServerInfo> getServers() {
    return servers;
  }
}
