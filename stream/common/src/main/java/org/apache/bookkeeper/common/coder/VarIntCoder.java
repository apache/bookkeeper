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
package org.apache.bookkeeper.common.coder;

import static com.google.common.base.Preconditions.checkNotNull;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import java.io.IOException;
import org.apache.bookkeeper.common.util.VarInt;

/**
 * A {@link Coder} that encodes {@link Integer Integers} using between 1 and 5 bytes. Negative
 * numbers always take 5 bytes.
 */
public class VarIntCoder implements Coder<Integer> {

    private static final long serialVersionUID = 465214482437322885L;

    public static VarIntCoder of() {
        return INSTANCE;
    }

    private static final VarIntCoder INSTANCE = new VarIntCoder();

    private VarIntCoder() {
    }

    @Override
    public void encode(Integer value, ByteBuf buf) {
        checkNotNull(value, "Can not encode a null integer value");
        checkNotNull(buf, "Can not encode into a null output buffer");

        ByteBufOutputStream output = new ByteBufOutputStream(buf);

        try {
            VarInt.encode(value.intValue(), output);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode integer '" + value + "' into the provided buffer", e);
        }
    }

    @Override
    public int getSerializedSize(Integer value) {
        return VarInt.getLength(value);
    }

    @Override
    public Integer decode(ByteBuf buf) {
        checkNotNull(buf, "Can not decode into a null input buffer");

        ByteBufInputStream input = new ByteBufInputStream(buf);

        try {
            return VarInt.decodeInt(input);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to decode an integration from the provided buffer");
        }
    }

}
