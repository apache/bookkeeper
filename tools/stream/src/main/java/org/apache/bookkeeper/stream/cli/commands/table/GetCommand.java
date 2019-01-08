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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.bookkeeper.stream.cli.Commands.OP_GET;

import com.beust.jcommander.Parameter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.api.kv.result.KeyValue;
import org.apache.bookkeeper.stream.cli.commands.ClientCommand;
import org.apache.bookkeeper.stream.cli.commands.table.GetCommand.Flags;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;

/**
 * Command to get kv.
 */
public class GetCommand extends ClientCommand<Flags> {

    private static final String NAME = OP_GET;
    private static final String DESC = "Get key/value pair from a table";

    /**
     * Flags for the get kv command.
     */
    public static class Flags extends CliFlags {

        @Parameter(names = { "-w", "--watch" }, description = "watch the value changes of a key")
        private boolean watch = false;

    }

    public GetCommand() {
        super(CliSpec.<Flags>newBuilder()
            .withName(NAME)
            .withDescription(DESC)
            .withFlags(new Flags())
            .withArgumentsUsage("<table> <key>")
            .build());
    }

    @Override
    protected void run(StorageClient client, Flags flags) throws Exception {
        checkArgument(flags.arguments.size() >= 2,
            "table and key are not provided");

        String tableName = flags.arguments.get(0);
        String key = flags.arguments.get(1);

        try (Table<ByteBuf, ByteBuf> table = result(client.openTable(tableName))) {
            long lastVersion = -1L;
            do {
                try (KeyValue<ByteBuf, ByteBuf> kv =
                         result(table.getKv(Unpooled.wrappedBuffer(key.getBytes(UTF_8))))) {
                    if (null == kv) {
                        spec.console().println("key '" + key + "' doesn't exist.");
                    } else {
                        if (kv.version() > lastVersion) {
                            if (kv.isNumber()) {
                                spec.console().println("value = " + kv.numberValue());
                            } else {
                                spec.console()
                                    .println("value = " + new String(ByteBufUtil.getBytes(kv.value()), UTF_8));
                            }
                            lastVersion = kv.version();
                        }
                    }
                }
                if (flags.watch) {
                    Thread.sleep(1000);
                }
            } while (flags.watch);
        }
    }

}
