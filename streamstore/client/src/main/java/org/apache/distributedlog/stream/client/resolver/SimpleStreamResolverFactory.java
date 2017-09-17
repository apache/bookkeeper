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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import io.grpc.Attributes;
import io.grpc.NameResolver;
import io.grpc.internal.GrpcUtil;
import java.net.URI;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.distributedlog.stream.proto.common.Endpoint;

/**
 * A simple resolver factory that creates simple name resolver.
 */
public class SimpleStreamResolverFactory extends AbstractStreamResolverFactory {

  private static final String SCHEME = "stream";
  private static final String NAME = "simple";

  public static final SimpleStreamResolverFactory of(List<Endpoint> endpoints) {
    return new SimpleStreamResolverFactory(endpoints);
  }

  public static final SimpleStreamResolverFactory of(Endpoint... endpoints) {
    return new SimpleStreamResolverFactory(Lists.newArrayList(endpoints));
  }

  private final List<URI> endpointURIs;

  private SimpleStreamResolverFactory(List<Endpoint> endpoints) {
    this.endpointURIs = Lists.transform(
      endpoints,
      new Function<Endpoint, URI>() {
        @Override
        public URI apply(Endpoint endpoint) {
          return URI.create("stream://" + endpoint.getHostname() + ":" + endpoint.getPort());
        }
      }
    );
  }

  @Override
  public String name() {
    return NAME;
  }

  @Nullable
  @Override
  public NameResolver newNameResolver(URI targetUri, Attributes params) {
    if (!Objects.equal(SCHEME, targetUri.getScheme())) {
      return null;
    }
    String targetPath = checkNotNull(targetUri.getPath());
    checkArgument(targetPath.startsWith("/"),
      "the path component (%s) of the target (%s) must start with '/'",
      targetPath, targetUri);
    String name = targetPath.substring(1);
    return new SimpleStreamResolver(name, GrpcUtil.SHARED_CHANNEL_EXECUTOR, this.endpointURIs);
  }

  @Override
  public String getDefaultScheme() {
    return SCHEME;
  }
}
