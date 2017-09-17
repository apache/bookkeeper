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

package org.apache.distributedlog.stream.client;

import static com.google.common.base.Preconditions.checkArgument;

import io.grpc.ManagedChannelBuilder;
import io.grpc.NameResolver;
import java.util.List;
import java.util.Optional;
import org.apache.distributedlog.stream.api.StreamClient;
import org.apache.distributedlog.stream.proto.common.Endpoint;
import org.inferred.freebuilder.FreeBuilder;

/**
 * Settings to configure a {@link StreamClient} client.
 */
@FreeBuilder
public interface StreamSettings {

  /**
   * Returns the number of worker threads in the core scheduler used by the client.
   *
   * @return the number of worker threads.
   */
  int numWorkerThreads();

  /**
   * Returns the name resolver factory used by zstream client.
   *
   * @return name resolver factory.
   */
  Optional<NameResolver.Factory> nameResolverFactory();

  /**
   * Returns the endpoints used by the client builder.
   *
   * @return the list of endpoints.
   */
  List<Endpoint> endpoints();

  /**
   * Returns the builder to create the managed channel.
   *
   * @return
   */
  Optional<ManagedChannelBuilder> managedChannelBuilder();

  /**
   * Use of a plaintext connection to the server. By default a secure connection mechanism
   * such as TLS will be used.
   *
   * <p>Should only be used for testing or for APIs where the use of such API or the data
   * exchanged is not sensitive.
   *
   * @return true if use a plaintext connection to the server, otherwise false.
   */
  boolean usePlaintext();

  /**
   * Configure the client name.
   *
   * @return client name.
   */
  Optional<String> clientName();

  /**
   * Builder of {@link StreamSettings} instances.
   */
  class Builder extends StreamSettings_Builder {

    Builder() {
      numWorkerThreads(Runtime.getRuntime().availableProcessors());
      usePlaintext(true);
    }

    @Override
    public StreamSettings build() {
      checkArgument(nameResolverFactory().isPresent() || !endpoints().isEmpty() || managedChannelBuilder().isPresent(),
        "No name resolver or endpoints or channel builder provided");
      return super.build();
    }

  }

  static Builder newBuilder() {
    // builder with default values
    return new Builder();
  }

}
