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

import java.io.Serializable;
import org.apache.bookkeeper.common.router.HashRouter;
import org.inferred.freebuilder.FreeBuilder;

/**
 * StreamClient Config.
 */
@FreeBuilder
public interface StreamConfig<KeyT, ValueT> extends Serializable {

  /**
   * The hash router used by the stream to route the keys.
   *
   * @return hash router used by the stream to route the keys to ranges.
   */
  HashRouter<KeyT> hashRouter();

  /**
   * Build to construct the stream configuration.
   *
   * @param <KeyT> key type
   * @param <ValueT> value type
   */
  class Builder<KeyT, ValueT> extends StreamConfig_Builder<KeyT, ValueT> {}

  /**
   * Builder to construct the stream config.
   *
   * @return stream config builder.
   */
  static Builder newBuilder() {
    return new Builder();
  }

}
