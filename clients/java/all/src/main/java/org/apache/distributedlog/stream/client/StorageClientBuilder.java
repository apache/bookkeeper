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
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.validateCollectionName;

import java.util.function.Supplier;
import org.apache.distributedlog.api.StorageClient;
import org.apache.distributedlog.stream.client.impl.StorageClientImpl;

/**
 * Builder to build a {@link StorageClient} client.
 */
public class StorageClientBuilder implements Supplier<StorageClient> {

  private StorageClientSettings settings = null;
  private String collectionName = null;

  /**
   * Create a builder to build {@link StorageClient} clients.
   *
   * @return StorageClient builder
   */
  public static StorageClientBuilder newBuilder() {
    return new StorageClientBuilder();
  }

  private StorageClientBuilder() {}

  /**
   * Configure the client with {@link StorageClientSettings}.
   *
   * @param settings stream withSettings
   * @return stream client builder
   */
  public StorageClientBuilder withSettings(StorageClientSettings settings) {
    this.settings = settings;
    return this;
  }

  /**
   * Configure the collection that the client will interact with.
   *
   * <p>The collection name will be used for building the stream client for interacting with streams
   * within the collection.
   *
   * @param colName colletion name
   * @return stream client builder.
   * @see #build()
   */
  public StorageClientBuilder withCollection(String colName) {
    this.collectionName = colName;
    return this;
  }

  /**
   * Build a {@link StorageClient} client.
   *
   * @return a {@link StorageClient} client.
   */
  public StorageClient build() {
    checkNotNull(settings, "Stream settings is null");
    checkArgument(validateCollectionName(collectionName), "Collection name is invalid");

    return new StorageClientImpl(
      collectionName,
      settings,
      ClientResources.create());
  }

  @Override
  public StorageClient get() {
    return build();
  }
}
