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
package org.apache.bookkeeper.clients.impl.kv;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.api.kv.PTable;
import org.apache.bookkeeper.api.kv.impl.options.OptionFactoryImpl;
import org.apache.bookkeeper.api.kv.options.DeleteOption;
import org.apache.bookkeeper.api.kv.options.IncrementOption;
import org.apache.bookkeeper.api.kv.options.OptionFactory;
import org.apache.bookkeeper.api.kv.options.PutOption;
import org.apache.bookkeeper.api.kv.options.RangeOption;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link ByteBufTableImpl}.
 */
public class ByteBufTableImplTest {

    private PTable<ByteBuf, ByteBuf> pTable;
    private ByteBuf key;
    private ByteBuf value;
    private ByteBufTableImpl table;
    private OptionFactory<ByteBuf> optionFactory;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() {
        pTable = mock(PTable.class);
        key = Unpooled.wrappedBuffer("test-key".getBytes(UTF_8));
        value = Unpooled.wrappedBuffer("test-value".getBytes(UTF_8));
        table = new ByteBufTableImpl(pTable);
        optionFactory = new OptionFactoryImpl<>();
    }

    @Test
    public void testGet() {
        try (RangeOption<ByteBuf> option = optionFactory.newRangeOption().build()) {
            table.get(key, option);
            verify(pTable, times(1))
                .get(same(key), same(key), same(option));
        }
    }

    @Test
    public void testPut() {
        try (PutOption<ByteBuf> option = optionFactory.newPutOption().build()) {
            table.put(key, value, option);
            verify(pTable, times(1))
                .put(same(key), same(key), same(value), same(option));
        }
    }

    @Test
    public void testDelete() {
        try (DeleteOption<ByteBuf> option = optionFactory.newDeleteOption().build()) {
            table.delete(key, option);
            verify(pTable, times(1))
                .delete(same(key), same(key), same(option));
        }
    }

    @Test
    public void testIncrement() {
        try (IncrementOption<ByteBuf> option = optionFactory.newIncrementOption().build()) {
            table.increment(key, 100L, option);
            verify(pTable, times(1))
                .increment(same(key), same(key), eq(100L), same(option));
        }
    }

    @Test
    public void testTxn() {
        table.txn(key);
        verify(pTable, times(1)).txn(same(key));
    }

    @Test
    public void testOpFactory() {
        table.opFactory();
        verify(pTable, times(1)).opFactory();
    }

    @Test
    public void testClose() {
        table.close();
        verify(pTable, times(1)).close();
    }

}
