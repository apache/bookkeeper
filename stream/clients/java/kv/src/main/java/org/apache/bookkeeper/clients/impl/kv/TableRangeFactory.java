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

package org.apache.bookkeeper.clients.impl.kv;

import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.api.kv.PTable;
import org.apache.bookkeeper.api.kv.impl.result.KeyValueFactory;
import org.apache.bookkeeper.api.kv.impl.result.ResultFactory;
import org.apache.bookkeeper.api.kv.op.OpFactory;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.proto.StreamProperties;

/**
 * Factory to open a range for a table.
 */
@FunctionalInterface
public interface TableRangeFactory<K, V> {

    PTable<K, V> openTableRange(StreamProperties streamProps,
                                RangeProperties rangeProps,
                                ScheduledExecutorService executor,
                                OpFactory<K, V> opFactory,
                                ResultFactory<K, V> resultFactory,
                                KeyValueFactory<K, V> kvFactory);

}
