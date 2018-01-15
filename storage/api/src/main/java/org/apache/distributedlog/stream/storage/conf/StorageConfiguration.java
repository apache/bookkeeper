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

import java.io.File;
import org.apache.bookkeeper.common.conf.ComponentConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;

/**
 * Storage related configuration settings.
 */
public class StorageConfiguration extends ComponentConfiguration {

  private static final String COMPONENT_PREFIX = "storage" + DELIMITER;

  private static final String RANGE_STORE_DIRS = "range_store_dirs";

  public StorageConfiguration(CompositeConfiguration conf) {
    super(conf, COMPONENT_PREFIX);
  }

  private String[] getRangeStoreDirNames() {
    String[] rangeStoreDirs = getStringArray(getKeyName(RANGE_STORE_DIRS));
    if (null == rangeStoreDirs || 0 == rangeStoreDirs.length) {
      return new String[] { "data/bookkeeper/ranges" };
    } else {
      return rangeStoreDirs;
    }
  }

  public File[] getRangeStoreDirs() {
    String[] rangeStoreDirNames = getRangeStoreDirNames();

    File[] rangeStoreDirs = new File[rangeStoreDirNames.length];
    for (int i = 0; i < rangeStoreDirNames.length; i++) {
      rangeStoreDirs[i] = new File(rangeStoreDirNames[i]);
    }
    return rangeStoreDirs;
  }

}
