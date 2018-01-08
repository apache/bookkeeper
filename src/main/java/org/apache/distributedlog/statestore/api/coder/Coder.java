/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.distributedlog.statestore.api.coder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.Serializable;

/**
 * A {@link Coder Coder&lt;T&gt;} defines how to encode and decode values of type {@code T} into
 * byte streams.
 *
 * @param <T> the type parameter
 */
public interface Coder<T> extends Serializable {

    /**
     * Encodes the given value of type {@code T} onto the given output buffer.
     *
     * @param value  value to encode
     * @return the serialized bytes buf.
     */
    default ByteBuf encodeBuf(T value) {
        return Unpooled.wrappedBuffer(encode(value));
    }

    /**
     * Encodes the given value of type {@code T} onto a bytes array.
     *
     * @param value value to encode
     * @return the serialized bytes arrya.
     */
    byte[] encode(T value);

    /**
     * Decode a value of type {@code T} from the given input buffer.
     * Returns the decoded value.
     *
     * @param data the input buffer
     * @return the decoded value.
     */
    T decode(ByteBuf data);

    /**
     * Decode a value of type {@code T} from the given bytes array.
     *
     * @param data bytes array
     * @return the decoded value.
     */
    default T decode(byte[] data) {
        return decode(Unpooled.wrappedBuffer(data));
    }

}
