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

import org.inferred.freebuilder.FreeBuilder;

/**
 * Put option.
 */
@FreeBuilder
public interface PutOption {

  boolean prevKv();

  long leaseId();

  long expectedVersion();

  /**
   * Builder to build put option.
   */
  class Builder extends PutOption_Builder {

    Builder() {
      prevKv(false);
      leaseId(0L);
      expectedVersion(-1);
    }

  }

  static Builder newBuilder() {
    return new Builder();
  }

}
