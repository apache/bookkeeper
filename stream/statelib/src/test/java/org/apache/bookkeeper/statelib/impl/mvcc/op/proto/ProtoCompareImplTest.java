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

package org.apache.bookkeeper.statelib.impl.mvcc.op.proto;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.protobuf.ByteString;
import lombok.Cleanup;
import org.apache.bookkeeper.stream.proto.kv.rpc.Compare;
import org.apache.bookkeeper.stream.proto.kv.rpc.Compare.CompareResult;
import org.apache.bookkeeper.stream.proto.kv.rpc.Compare.CompareTarget;
import org.junit.Test;

/**
 * Unit test {@link ProtoCompareImpl}.
 */
public class ProtoCompareImplTest {

    private static final ByteString KEY = ByteString.copyFromUtf8("test-key");
    private static final ByteString VAL = ByteString.copyFromUtf8("test-value");
    private static final long MOD_REV = System.currentTimeMillis();
    private static final long CREATE_REV = MOD_REV + 1;
    private static final long VERSION = CREATE_REV + 1;

    @Test
    public void testCompareEmptyValue() {
        Compare compare = Compare.newBuilder()
            .setKey(KEY)
            .setResult(CompareResult.EQUAL)
            .setTarget(CompareTarget.VALUE)
            .build();

        @Cleanup ProtoCompareImpl protoCompare = ProtoCompareImpl.newCompareOp(compare);
        assertArrayEquals("test-key".getBytes(UTF_8), protoCompare.key());
        assertNull(protoCompare.value());
        assertEquals(org.apache.bookkeeper.api.kv.op.CompareResult.EQUAL, protoCompare.result());
        assertEquals(org.apache.bookkeeper.api.kv.op.CompareTarget.VALUE, protoCompare.target());
    }

    @Test
    public void testCompareValue() {
        Compare compare = Compare.newBuilder()
            .setKey(KEY)
            .setValue(VAL)
            .setResult(CompareResult.EQUAL)
            .setTarget(CompareTarget.VALUE)
            .build();

        @Cleanup ProtoCompareImpl protoCompare = ProtoCompareImpl.newCompareOp(compare);
        assertArrayEquals("test-key".getBytes(UTF_8), protoCompare.key());
        assertArrayEquals("test-value".getBytes(UTF_8), protoCompare.value());
        assertEquals(org.apache.bookkeeper.api.kv.op.CompareResult.EQUAL, protoCompare.result());
        assertEquals(org.apache.bookkeeper.api.kv.op.CompareTarget.VALUE, protoCompare.target());
    }

    @Test
    public void testCompareMod() {
        Compare compare = Compare.newBuilder()
            .setKey(KEY)
            .setModRevision(MOD_REV)
            .setResult(CompareResult.EQUAL)
            .setTarget(CompareTarget.MOD)
            .build();

        @Cleanup ProtoCompareImpl protoCompare = ProtoCompareImpl.newCompareOp(compare);
        assertArrayEquals("test-key".getBytes(UTF_8), protoCompare.key());
        assertEquals(MOD_REV, protoCompare.revision());
        assertEquals(org.apache.bookkeeper.api.kv.op.CompareResult.EQUAL, protoCompare.result());
        assertEquals(org.apache.bookkeeper.api.kv.op.CompareTarget.MOD, protoCompare.target());
    }

    @Test
    public void testCompareCreate() {
        Compare compare = Compare.newBuilder()
            .setKey(KEY)
            .setCreateRevision(CREATE_REV)
            .setResult(CompareResult.EQUAL)
            .setTarget(CompareTarget.CREATE)
            .build();

        @Cleanup ProtoCompareImpl protoCompare = ProtoCompareImpl.newCompareOp(compare);
        assertArrayEquals("test-key".getBytes(UTF_8), protoCompare.key());
        assertEquals(CREATE_REV, protoCompare.revision());
        assertEquals(org.apache.bookkeeper.api.kv.op.CompareResult.EQUAL, protoCompare.result());
        assertEquals(org.apache.bookkeeper.api.kv.op.CompareTarget.CREATE, protoCompare.target());
    }

    @Test
    public void testCompareVersion() {
        Compare compare = Compare.newBuilder()
            .setKey(KEY)
            .setVersion(VERSION)
            .setResult(CompareResult.EQUAL)
            .setTarget(CompareTarget.VERSION)
            .build();

        @Cleanup ProtoCompareImpl protoCompare = ProtoCompareImpl.newCompareOp(compare);
        assertArrayEquals("test-key".getBytes(UTF_8), protoCompare.key());
        assertEquals(VERSION, protoCompare.revision());
        assertEquals(org.apache.bookkeeper.api.kv.op.CompareResult.EQUAL, protoCompare.result());
        assertEquals(org.apache.bookkeeper.api.kv.op.CompareTarget.VERSION, protoCompare.target());
    }

}
