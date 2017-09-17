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

package org.apache.distributedlog.stream.api.view.kv;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.distributedlog.stream.api.view.kv.options.GetOption;
import org.apache.distributedlog.stream.api.view.kv.result.GetResult;

/**
 * A review view of a key/value space.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface ReadView extends AutoCloseable {

  CompletableFuture<GetResult> get(ByteBuf pKey, ByteBuf lKey, GetOption option);

  void close();

}
