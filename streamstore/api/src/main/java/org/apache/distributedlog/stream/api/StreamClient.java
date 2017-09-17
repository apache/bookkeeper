/**
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
package org.apache.distributedlog.stream.api;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.distributedlog.stream.api.view.kv.Table;

/**
 * StreamClient is the client that application uses to open streams and tables,
 * and other different readers to access the stream in different materialized views.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface StreamClient extends StreamReadClient {

  /**
   * Open a stream as a key/value view.
   *
   * @param streamName stream name
   * @return the key/value view of a stream.
   */
  CompletableFuture<Table> openStreamAsTable(String streamName);

}
