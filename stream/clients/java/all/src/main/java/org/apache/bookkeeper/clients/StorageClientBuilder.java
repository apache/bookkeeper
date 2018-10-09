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
package org.apache.bookkeeper.clients;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.validateNamespaceName;

import java.util.function.Supplier;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.clients.admin.SimpleStorageAdminClientImpl;
import org.apache.bookkeeper.clients.admin.StorageAdminClient;
import org.apache.bookkeeper.clients.admin.StorageAdminClientImpl;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.utils.ClientResources;

/**
 * Builder to build a {@link StorageClient} client.
 */
public class StorageClientBuilder implements Supplier<StorageClient> {

    private StorageClientSettings settings = null;
    private String namespaceName = null;

    /**
     * Create a builder to build {@link StorageClient} clients.
     *
     * @return StorageClient builder
     */
    public static StorageClientBuilder newBuilder() {
        return new StorageClientBuilder();
    }

    private StorageClientBuilder() {
    }

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
     * Configure the namespace that the client will interact with.
     *
     * <p>The namespace name will be used for building the stream client for interacting with streams
     * within the namespace.
     *
     * @param colName colletion name
     * @return stream client builder.
     * @see #build()
     */
    public StorageClientBuilder withNamespace(String colName) {
        this.namespaceName = colName;
        return this;
    }

    /**
     * Build a {@link StorageClient} client.
     *
     * @return a {@link StorageClient} client.
     */
    public StorageClient build() {
        checkNotNull(settings, "Stream settings is null");
        checkArgument(validateNamespaceName(namespaceName), "Namespace name '" + namespaceName + "'is invalid");

        if (settings.enableServerSideRouting()) {
            return new SimpleStorageClientImpl(namespaceName, settings);
        } else {
            return new StorageClientImpl(
                namespaceName,
                settings,
                ClientResources.create());
        }
    }

    /**
     * Build a {@link StorageAdminClient} client.
     *
     * @return a {@link StorageAdminClient} client.
     */
    public StorageAdminClient buildAdmin() {
        checkNotNull(settings, "Storage client settings is null");

        if (settings.enableServerSideRouting()) {
            return new SimpleStorageAdminClientImpl(settings);
        } else {
            return new StorageAdminClientImpl(settings, ClientResources.create());
        }
    }

    @Override
    public StorageClient get() {
        return build();
    }
}
