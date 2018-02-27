/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.clients.admin;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.util.AutoAsyncCloseable;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.NamespaceProperties;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.bookkeeper.stream.proto.StreamProperties;

/**
 * A storage admin client.
 */
public interface StorageAdminClient extends AutoAsyncCloseable {

    CompletableFuture<NamespaceProperties> createNamespace(String namespace,
                                                           NamespaceConfiguration conf);

    CompletableFuture<Boolean> deleteNamespace(String namespace);

    CompletableFuture<NamespaceProperties> getNamespace(String namespace);

    CompletableFuture<StreamProperties> createStream(String namespace,
                                                     String streamName,
                                                     StreamConfiguration streamConfiguration);

    CompletableFuture<Boolean> deleteStream(String namespace, String streamName);

    CompletableFuture<StreamProperties> getStream(String namespace, String streamName);

}
