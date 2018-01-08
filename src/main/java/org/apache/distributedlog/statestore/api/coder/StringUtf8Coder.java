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

import static com.google.common.base.Charsets.UTF_8;

import io.netty.buffer.ByteBuf;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.distributedlog.common.coder.Coder;
import org.apache.distributedlog.common.util.ByteBufUtils;

/**
 * A coder that encodes strings in utf-8 format.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class StringUtf8Coder implements Coder<String> {

    /**
     * Get the coder to encode strings in utf-8.
     *
     * @return string coder.
     */
    public static StringUtf8Coder of() {
        return INSTANCE;
    }

    private static final StringUtf8Coder INSTANCE = new StringUtf8Coder();

    @Override
    public byte[] encode(String value) {
        return value.getBytes(UTF_8);
    }

    @Override
    public String decode(byte[] data) {
        return new String(data, UTF_8);
    }

    @Override
    public String decode(ByteBuf data) {
        byte[] bytes = ByteBufUtils.getArray(data);
        return decode(bytes);
    }
}
