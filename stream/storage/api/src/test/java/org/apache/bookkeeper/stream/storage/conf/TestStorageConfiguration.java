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
package org.apache.bookkeeper.stream.storage.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.commons.configuration2.CompositeConfiguration;
import org.junit.Test;

/**
 * Unit tests for {@link StorageConfiguration}.
 */
public class TestStorageConfiguration {

    @Test(timeout = 60000)
    public void testGetStorageSettings() {
        CompositeConfiguration conf = new CompositeConfiguration();
        conf.setProperty("xxx.key", "xxx.value");
        conf.setProperty("storage.key", "storage.value");
        conf.setProperty("storage-key", "storage-value");

        StorageConfiguration storageConf = new StorageConfiguration(conf);
        assertEquals("storage.value", storageConf.getString("key"));
        assertTrue(storageConf.containsKey("key"));
        assertFalse(storageConf.containsKey("xxx.key"));
        assertFalse(storageConf.containsKey("storage.key"));
        assertFalse(storageConf.containsKey("storage-key"));
    }

}
