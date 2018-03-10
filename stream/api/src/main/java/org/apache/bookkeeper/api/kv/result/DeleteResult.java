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
package org.apache.bookkeeper.api.kv.result;

import java.util.List;

/**
 * Delete result.
 */
public interface DeleteResult<K, V> extends Result<K, V> {

    /**
     * Returns the number of kv pairs deleted.
     *
     * @return the number of kv pairs deleted.
     */
    long numDeleted();

    /**
     * Returns the list of previous kv pairs of the keys
     * deleted in ths op.
     *
     * @return the list of previous kv pairs.
     */
    List<KeyValue<K, V>> prevKvs();

    /**
     * Get the list of previous kv pairs and clear them from the result.
     *
     * @return the list of previous kv pairs.
     */
    List<KeyValue<K, V>> getPrevKvsAndClear();

}
