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
package org.apache.bookkeeper.tools.cli.commands.client;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.api.CreateBuilder;
import org.apache.bookkeeper.client.api.DeleteBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.tools.cli.helpers.ClientCommandTestBase;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit test of {@link SimpleTestCommand}.
 */
public class SimpleTestCommandTest extends ClientCommandTestBase {

    @Test
    public void testCommandShortArgs() throws Exception {
        testCommand(
            "-e", "5",
            "-w", "3",
            "-a", "3",
            "-n", "10");
    }

    @Test
    public void testCommandLongArgs() throws Exception {
        testCommand(
            "--ensemble-size", "5",
            "--write-quorum-size", "3",
            "--ack-quorum-size", "3",
            "--num-entries", "10");
    }

    @SuppressWarnings("unchecked")
    public void testCommand(String... args) throws Exception {
        WriteHandle wh = mock(WriteHandle.class);
        AtomicLong counter = new AtomicLong(0L);
        CreateBuilder createBuilder = mock(CreateBuilder.class);
        when(createBuilder.execute()).thenReturn(FutureUtils.value(wh));
        when(createBuilder.withEnsembleSize(anyInt())).thenReturn(createBuilder);
        when(createBuilder.withCustomMetadata(any())).thenReturn(createBuilder);
        when(createBuilder.withWriteQuorumSize(anyInt())).thenReturn(createBuilder);
        when(createBuilder.withAckQuorumSize(anyInt())).thenReturn(createBuilder);
        when(createBuilder.withDigestType(any(DigestType.class))).thenReturn(createBuilder);
        when(createBuilder.withPassword(any(byte[].class))).thenReturn(createBuilder);
        when(createBuilder.execute()).thenReturn(CompletableFuture.completedFuture(wh));
        when(mockBk.newCreateLedgerOp()).thenReturn(createBuilder);
        long ledgerId = 1234L;
        when(wh.getId()).thenReturn(ledgerId);
        when(wh.getLastAddPushed()).then(__ -> counter.get() - 1L);
        List<LedgerEntry> entries = new ArrayList<>();
        byte[] data = new byte[100]; // test data
        Random random = new Random(0);
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (random.nextInt(26) + 65);
        }
        when(wh.append(any(byte[].class))).then(invocation -> {
            long entryId = counter.getAndIncrement();
            ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.heapBuffer(100);
            buffer.writeBytes(data);
            entries.add(LedgerEntryImpl.create(ledgerId, entryId, data.length, buffer));
            return entryId;
        });
        ReadHandle rh = mock(ReadHandle.class);
        when(rh.read(anyLong(), anyLong())).then(
                __ -> LedgerEntriesImpl.create(entries.stream()
                        .map(LedgerEntry::duplicate).collect(Collectors.toList())));
        when(rh.readUnconfirmed(anyLong(), anyLong())).then(
                __ -> LedgerEntriesImpl.create(entries.stream()
                        .map(LedgerEntry::duplicate).collect(Collectors.toList())));
        OpenBuilder openBuilder = mock(OpenBuilder.class);
        when(openBuilder.withLedgerId(anyLong())).thenReturn(openBuilder);
        when(openBuilder.withDigestType(any())).thenReturn(openBuilder);
        when(openBuilder.withPassword(any())).thenReturn(openBuilder);
        when(openBuilder.execute()).thenReturn(CompletableFuture.completedFuture(rh));
        when(mockBk.newOpenLedgerOp()).thenReturn(openBuilder);

        DeleteBuilder deleteBuilder = mock(DeleteBuilder.class);
        when(deleteBuilder.withLedgerId(anyLong())).thenReturn(deleteBuilder);
        when(deleteBuilder.execute()).thenReturn(CompletableFuture.completedFuture(null));
        when(mockBk.newDeleteLedgerOp()).thenReturn(deleteBuilder);

        SimpleTestCommand cmd = new SimpleTestCommand();
        cmd.apply(bkFlags, args);

        // verify create builder
        ArgumentCaptor<Map> mapArgumentCaptor = ArgumentCaptor.forClass(Map.class);
        verify(createBuilder, times(1)).withEnsembleSize(eq(5));
        verify(createBuilder, times(1)).withWriteQuorumSize(eq(3));
        verify(createBuilder, times(1)).withAckQuorumSize(eq(3));
        verify(createBuilder, times(1)).withCustomMetadata(mapArgumentCaptor.capture());
        assertTrue(Arrays.equals((byte[]) mapArgumentCaptor.getValue().get("Bookie"),
                "simpletest".getBytes(StandardCharsets.UTF_8)));
        verify(createBuilder, times(1)).withDigestType(eq(DigestType.CRC32C));
        verify(createBuilder, times(1)).withPassword(eq(new byte[0]));
        verify(createBuilder, times(1)).execute();

        verify(openBuilder, times(2)).withLedgerId(eq(1234L));
        verify(openBuilder, times(2)).execute();

        verify(deleteBuilder, times(1)).withLedgerId(eq(1234L));
        verify(deleteBuilder, times(1)).execute();

        // verify appends
        verify(wh, times(10)).append(eq(data));

        // verify close ledger handle.
        verify(wh, times(1)).close();

        // verify read entry 0-9
        verify(rh, times(1)).readUnconfirmed(0, 9);
        verify(rh, times(1)).read(0, 9);
    }

}
