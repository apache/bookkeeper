/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.statelib.impl.kv;

import static org.junit.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import lombok.Cleanup;
import org.apache.bookkeeper.proto.statestore.kv.Command;
import org.junit.Test;

/**
 * Unit test for {@link KVUtils}.
 */
public class TestKVUtils {

    @Test
    public void testNewLogRecordBuf() throws Exception {
        Command command = KVUtils.NOP_CMD;
        @Cleanup("release") ByteBuf buffer = KVUtils.newCommandBuf(command);
        assertEquals(command.getSerializedSize(), buffer.readableBytes());
        Command another = KVUtils.newCommand(buffer);
        assertEquals(command, another);
    }

}
