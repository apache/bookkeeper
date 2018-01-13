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
package org.apache.distributedlog.stream.storage.conf;

import org.apache.bookkeeper.common.conf.ComponentConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;

/**
 * Storage related configuration settings.
 */
public class StorageConfiguration extends ComponentConfiguration {

  private static final String COMPONENT_PREFIX = "storage" + DELIMITER;

  public StorageConfiguration(CompositeConfiguration conf) {
    super(conf, COMPONENT_PREFIX);
  }

}
