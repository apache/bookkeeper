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
package org.apache.distributedlog.stream.cli.commands.table;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.distributedlog.api.StorageClient;
import org.apache.distributedlog.api.kv.Table;
import org.apache.distributedlog.api.kv.result.KeyValue;
import org.apache.distributedlog.stream.cli.commands.ClientCommand;

/**
 * Command to get kv.
 */
@Parameters(commandDescription = "Get key/value pair from a table")
public class GetCommand extends ClientCommand {

    @Parameter(names = { "-t", "--table" }, description = "table name")
    private String tableName = null;

    @Parameter(names = { "-k", "--key" }, description = "key")
    private String key = null;

    @Parameter(names = { "-w", "--watch" }, description = "watch the value changes of a key")
    private boolean watch = false;

    @Override
    protected void run(StorageClient client) throws Exception {
        checkNotNull(tableName, "Table name is not provided");
        checkNotNull(key, "Key is not provided");

        try (Table<ByteBuf, ByteBuf> table = result(client.openTable(tableName))) {
            long lastVersion = -1L;
            do {
                try (KeyValue<ByteBuf, ByteBuf> kv = result(table.getKv(Unpooled.wrappedBuffer(key.getBytes(UTF_8))))) {
                    if (null == kv) {
                        System.out.println("key '" + key + "' doesn't exist.");
                    } else {
                        if (kv.version() > lastVersion) {
                            if (kv.isNumber()) {
                                System.out.println("value = " + kv.numberValue());
                            } else {
                                System.out.println("value = " + new String(ByteBufUtil.getBytes(kv.value()), UTF_8));
                            }
                            lastVersion = kv.version();
                        }
                    }
                }
                if (watch) {
                    Thread.sleep(1000);
                }
            } while (watch);
        }
    }

    @Override
    public String name() {
        return "get";
    }
}
