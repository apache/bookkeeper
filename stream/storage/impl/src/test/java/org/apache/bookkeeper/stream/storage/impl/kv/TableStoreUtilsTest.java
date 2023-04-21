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
package org.apache.bookkeeper.stream.storage.impl.kv;

import static org.apache.bookkeeper.stream.storage.impl.kv.TableStoreUtils.HAS_ROUTING_KEY;
import static org.apache.bookkeeper.stream.storage.impl.kv.TableStoreUtils.NO_ROUTING_KEY;
import static org.apache.bookkeeper.stream.storage.impl.kv.TableStoreUtils.SEP;
import static org.apache.bookkeeper.stream.storage.impl.kv.TableStoreUtils.getLKey;
import static org.apache.bookkeeper.stream.storage.impl.kv.TableStoreUtils.handleCause;
import static org.apache.bookkeeper.stream.storage.impl.kv.TableStoreUtils.hasRKey;
import static org.apache.bookkeeper.stream.storage.impl.kv.TableStoreUtils.newKeyValue;
import static org.apache.bookkeeper.stream.storage.impl.kv.TableStoreUtils.newStoreKey;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.api.kv.result.Code;
import org.apache.bookkeeper.api.kv.result.KeyValue;
import org.apache.bookkeeper.statelib.api.exceptions.MVCCStoreException;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.junit.Test;

/**
 * Unit test of {@link TableStoreUtils}.
 */
public class TableStoreUtilsTest {

    private final byte[] rKey = new byte[]{'a', 'b', 'c'};
    private final byte[] lKey = new byte[]{'d', 'e', 'e'};
    private final byte[] storeKeyWithRKey = new byte[]{
        HAS_ROUTING_KEY,
        'a', 'b', 'c',
        SEP,
        'd', 'e', 'e'
    };
    private final byte[] storeKeyWithoutRKey = new byte[]{
        NO_ROUTING_KEY,
        'd', 'e', 'e'
    };
    private final byte[] value = new byte[]{'v', 'a', 'l', 'u', 'e'};

    @Test
    public void testHasRKey() {
        assertFalse(hasRKey(null));
        assertFalse(hasRKey(ByteString.copyFrom(new byte[0])));
        assertTrue(hasRKey(ByteString.copyFromUtf8("test")));
    }

    @Test
    public void testNewStoreKey() {
        assertArrayEquals(
            storeKeyWithRKey,
            newStoreKey(
                ByteString.copyFrom(rKey),
                ByteString.copyFrom(lKey))
        );
        assertArrayEquals(
            storeKeyWithoutRKey,
            newStoreKey(
                null,
                ByteString.copyFrom(lKey))
        );
    }

    @Test
    public void testGetLKey() {
        assertEquals(
            ByteString.copyFrom(lKey),
            getLKey(storeKeyWithoutRKey, null));
        assertEquals(
            ByteString.copyFrom(lKey),
            getLKey(storeKeyWithRKey, ByteString.copyFrom(rKey)));
    }

    @Test
    public void testHandleCause() {
        StatusCode[] protoCodes = new StatusCode[]{
            StatusCode.SUCCESS,
            StatusCode.INTERNAL_SERVER_ERROR,
            StatusCode.BAD_REQUEST,
            StatusCode.BAD_REQUEST,
            StatusCode.UNEXPECTED,
            StatusCode.BAD_REVISION,
            StatusCode.BAD_REVISION,
            StatusCode.KEY_NOT_FOUND,
            StatusCode.KEY_EXISTS,
        };
        Code[] codes = new Code[]{
            Code.OK,
            Code.INTERNAL_ERROR,
            Code.INVALID_ARGUMENT,
            Code.ILLEGAL_OP,
            Code.UNEXPECTED,
            Code.BAD_REVISION,
            Code.SMALLER_REVISION,
            Code.KEY_NOT_FOUND,
            Code.KEY_EXISTS,
        };

        for (int i = 0; i < codes.length; i++) {
            Code code = codes[i];
            MVCCStoreException exception = new MVCCStoreException(code, "test-" + code);
            assertEquals(protoCodes[i], handleCause(exception));
        }
    }

    @Test
    public void testHandleUnknownCause() {
        Exception exception = new Exception("test-unknown-cause");
        assertEquals(StatusCode.INTERNAL_SERVER_ERROR, handleCause(exception));
    }

    @Test
    public void testHandleExecutionException() {
        MVCCStoreException exception = new MVCCStoreException(
            Code.BAD_REVISION, "bad-revision");
        ExecutionException ee = new ExecutionException(exception);
        assertEquals(StatusCode.BAD_REVISION, handleCause(ee));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNewKeyValue() {
        long rid = System.currentTimeMillis();
        long createRev = 2 * System.currentTimeMillis();
        long modRev = 3 * System.currentTimeMillis();
        long version = 4 * System.currentTimeMillis();
        KeyValue<byte[], byte[]> kv = mock(KeyValue.class);
        when(kv.key()).thenReturn(storeKeyWithRKey);
        when(kv.value()).thenReturn(this.value);
        when(kv.createRevision()).thenReturn(createRev);
        when(kv.modifiedRevision()).thenReturn(modRev);
        when(kv.version()).thenReturn(version);

        org.apache.bookkeeper.stream.proto.kv.KeyValue keyValue =
            newKeyValue(ByteString.copyFrom(rKey), kv);

        assertEquals(ByteString.copyFrom(lKey), keyValue.getKey());
        assertEquals(ByteString.copyFrom(value), keyValue.getValue());
        assertEquals(createRev, keyValue.getCreateRevision());
        assertEquals(modRev, keyValue.getModRevision());
        assertEquals(version, keyValue.getVersion());
    }

    //
    // TODO: add more test cases
    //

}
