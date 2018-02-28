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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.stream.cli.commands.ClientCommand;

/**
 * Commands to put kvs.
 */
@Parameters(commandDescription = "Put key/value pair to a table")
public class PutCommand extends ClientCommand {

    @Parameter(names = { "-t", "--table" }, description = "table name")
    private String tableName = null;

    @Parameter(names = { "-k", "--key" }, description = "key")
    private String key = null;

    @Parameter(names = { "-v", "--value" }, description = "value")
    private String value = null;

    @Override
    protected void run(StorageClient client) throws Exception {
        checkNotNull(tableName, "Table name is not provided");
        checkNotNull(key, "Key is not provided");
        checkNotNull(value, "Value is not provided");

        try (Table<ByteBuf, ByteBuf> table = result(client.openTable(tableName))) {
            result(table.put(
                Unpooled.wrappedBuffer(key.getBytes(UTF_8)),
                Unpooled.wrappedBuffer(value.getBytes(UTF_8))));
            System.out.println("Successfully update kv: ('" + key + "', '" + value + "').");
        }
    }

    @Override
    public String name() {
        return "put";
    }
}
