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

package org.apache.distributedlog.api.kv;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.distributedlog.api.kv.options.DeleteOption;
import org.apache.distributedlog.api.kv.options.PutOption;
import org.apache.distributedlog.api.kv.result.DeleteResult;
import org.apache.distributedlog.api.kv.result.PutResult;

/**
 * Write view of a given key space.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface WriteView extends AutoCloseable {

  CompletableFuture<PutResult> put(ByteBuf pKey, ByteBuf lKey, ByteBuf value, PutOption option);

  CompletableFuture<DeleteResult> delete(ByteBuf pKey, ByteBuf lKey, DeleteOption option);

  void close();

}
