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

import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithLedgerManagerFactory;

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * Print the metadata for a ledger.
 */
public class LedgerMetaDataCommand extends BookieCommand<LedgerMetaDataCommand.LedgerMetadataFlag> {

    private static final String NAME = "show";
    private static final String DESC = "Print the metadata for a ledger, or optionally dump to a file.";
    private static final String DEFAULT = "";
    private static final long DEFAULT_ID = -1L;

    private LedgerMetadataSerDe serDe = new LedgerMetadataSerDe();
    private LedgerIdFormatter ledgerIdFormatter;

    public LedgerMetaDataCommand() {
        this(new LedgerMetadataFlag());
    }

    public LedgerMetaDataCommand(LedgerIdFormatter ledgerIdFormatter) {
        this();
        this.ledgerIdFormatter = ledgerIdFormatter;
    }

    public LedgerMetaDataCommand(LedgerMetadataFlag flag) {
        super(CliSpec.<LedgerMetadataFlag>newBuilder()
                  .withName(NAME)
                  .withDescription(DESC)
                  .withFlags(flag)
                  .build());
    }

    /**
     * Flags for ledger metadata command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class LedgerMetadataFlag extends CliFlags {

        @Parameter(names = { "-l", "--ledgerid" }, description = "Ledger ID", required = true)
        private long ledgerId = DEFAULT_ID;

        @Parameter(names = { "-d", "--dumptofile" }, description = "Dump metadata for ledger, to a file")
        private String dumpToFile = DEFAULT;

        @Parameter(names = { "-r", "--restorefromefile" }, description = "Restore metadata for ledger, from a file")
        private String restoreFromFile = DEFAULT;

        @Parameter(names =  {"-lf", "--ledgeridformatter"}, description = "Set ledger id formatter")
        private String ledgerIdFormatter = DEFAULT;
    }

    @Override
    public boolean apply(ServerConfiguration conf, LedgerMetadataFlag cmdFlags) {
        if (!cmdFlags.ledgerIdFormatter.equals(DEFAULT) && ledgerIdFormatter == null) {
            this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(cmdFlags.ledgerIdFormatter, conf);
        } else if (ledgerIdFormatter == null) {
            this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(conf);
        }
        try {
            return handler(conf, cmdFlags);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private boolean handler(ServerConfiguration conf, LedgerMetadataFlag flag)
        throws MetadataException, ExecutionException {
        if (flag.ledgerId == DEFAULT_ID) {
            System.err.println("Must specific a ledger id");
            return false;
        }
        runFunctionWithLedgerManagerFactory(conf, mFactory -> {
            try (LedgerManager m = mFactory.newLedgerManager()) {
                if (!flag.dumpToFile.equals(DEFAULT)) {
                    Versioned<LedgerMetadata> md = m.readLedgerMetadata(flag.ledgerId).join();
                    Files.write(FileSystems.getDefault().getPath(flag.dumpToFile),
                                serDe.serialize(md.getValue()));
                } else if (!flag.restoreFromFile.equals(DEFAULT)) {
                    byte[] serialized = Files.readAllBytes(
                        FileSystems.getDefault().getPath(flag.restoreFromFile));
                    LedgerMetadata md = serDe.parseConfig(serialized, Optional.empty());
                    m.createLedgerMetadata(flag.ledgerId, md).join();
                } else {
                    printLedgerMetadata(flag.ledgerId, m.readLedgerMetadata(flag.ledgerId).get().getValue(), true);
                }
            } catch (Exception e) {
                throw new UncheckedExecutionException(e);
            }
            return null;
        });
        return true;
    }

    private void printLedgerMetadata(long ledgerId, LedgerMetadata md, boolean printMeta) {
        System.out.println("ledgerID: " + ledgerIdFormatter.formatLedgerId(ledgerId));
        if (printMeta) {
            System.out.println(md.toString());
        }
    }
}
