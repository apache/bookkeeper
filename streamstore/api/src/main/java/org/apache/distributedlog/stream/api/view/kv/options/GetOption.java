/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.distributedlog.stream.api.view.kv.options;

import io.netty.buffer.ByteBuf;
import java.util.Optional;
import org.inferred.freebuilder.FreeBuilder;

/**
 * Get Option.
 */
@FreeBuilder
public interface GetOption {

  long limit();

  long revision();

  boolean keysOnly();

  boolean countOnly();

  Optional<ByteBuf> endKey();

  /**
   * Builder to build get option.
   */
  class Builder extends GetOption_Builder {

    private Builder() {
      limit(0L);
      revision(0L);
      keysOnly(false);
      countOnly(false);
    }

  }

  static Builder newBuilder() {
    return new Builder();
  }

}
