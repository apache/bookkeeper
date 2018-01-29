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
package org.apache.bookkeeper.stream.cli.commands.table;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.kv.TableWriter;
import org.apache.bookkeeper.stream.cli.commands.ClientCommand;

/**
 * Command to get kv.
 */
@Parameters(commandDescription = "Append updates to mutate a table")
public class AppendCommand extends ClientCommand {

    @Parameter(names = { "-t", "--table" }, description = "table name")
    private String tableName = null;

    @Parameter(names = { "-k", "--key" }, description = "key")
    private String key = null;

    @Parameter(names = { "-v", "--value" }, description = "value")
    private String value = null;

    @Parameter(names = { "-a", "--amount" }, description = "amount to increment")
    private long amount = 0;

    @Parameter(names = { "--times" }, description = "times to append")
    private int times = 1;

    @Parameter(names = { "-s", "--sequence" }, description = "sequence id to identify this update")
    private long sequenceId = -1L;

    @Override
    protected void run(StorageClient client) throws Exception {
        checkNotNull(tableName, "Table name is not provided");
        checkNotNull(key, "Key is not provided");
        checkArgument(sequenceId >= 0L, "Invalid sequence id : " + sequenceId);

        try (TableWriter<ByteBuf, ByteBuf> table = result(client.openTableWriter(tableName))) {
            for (int i = 0; i < times; i++) {
                if (amount > 0) {
                    result(table.increment(
                        sequenceId + i,
                        Unpooled.wrappedBuffer(key.getBytes(UTF_8)),
                        amount));
                    System.out.println("Successfully append increment update: ('"
                        + key + "', amount = '" + amount + "').");
                } else {
                    if (value == null) {
                        result(table.write(
                            sequenceId + i,
                            Unpooled.wrappedBuffer(key.getBytes(UTF_8)),
                            null));
                        System.out.println("Successfully append deletion : ('" + key + "').");
                    } else {
                        result(table.write(
                            sequenceId + i,
                            Unpooled.wrappedBuffer(key.getBytes(UTF_8)),
                            Unpooled.wrappedBuffer(value.getBytes(UTF_8))));
                        System.out.println("Successfully append kv: ('" + key + "', '" + value + "').");
                    }
                }
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public String name() {
        return "append";
    }
}
