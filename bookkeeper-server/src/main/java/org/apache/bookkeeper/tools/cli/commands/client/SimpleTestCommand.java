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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.util.concurrent.TimeUnit;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.tools.cli.helpers.ClientCommand;

/**
 * A client command that simply tests if a cluster is healthy.
 */
@Accessors(fluent = true)
@Setter
@Parameters(commandDescription = "Simple test to create a ledger and write entries to it.")
public class SimpleTestCommand extends ClientCommand {

    @Parameter(names = { "-e", "--ensemble-size" }, description = "Ensemble size (default 3)")
    private int ensembleSize = 3;
    @Parameter(names = { "-w", "--write-quorum-size" }, description = "Write quorum size (default 2)")
    private int writeQuorumSize = 2;
    @Parameter(names = { "-a", "--ack-quorum-size" }, description = "Ack quorum size (default 2)")
    private int ackQuorumSize = 2;
    @Parameter(names = { "-n", "--num-entries" }, description = "Entries to write (default 100)")
    private int numEntries = 100;

    @Override
    public String name() {
        return "simpletest";
    }

    @Override
    protected void run(BookKeeper bk) throws Exception {
        byte[] data = new byte[100]; // test data

        try (WriteHandle wh = result(bk.newCreateLedgerOp()
            .withEnsembleSize(ensembleSize)
            .withWriteQuorumSize(writeQuorumSize)
            .withAckQuorumSize(ackQuorumSize)
            .withDigestType(DigestType.CRC32C)
            .withPassword(new byte[0])
            .execute())) {

            System.out.println("Ledger ID: " + wh.getId());
            long lastReport = System.nanoTime();
            for (int i = 0; i < numEntries; i++) {
                wh.append(data);
                if (TimeUnit.SECONDS.convert(System.nanoTime() - lastReport,
                        TimeUnit.NANOSECONDS) > 1) {
                    System.out.println(i + " entries written");
                    lastReport = System.nanoTime();
                }
            }
            System.out.println(numEntries + " entries written to ledger " + wh.getId());
        }
    }
}
