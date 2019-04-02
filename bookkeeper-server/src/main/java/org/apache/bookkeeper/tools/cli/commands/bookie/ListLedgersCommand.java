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
package org.apache.bookkeeper.tools.cli.commands.bookie;

import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithLedgerManagerFactory;

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.tools.cli.commands.bookie.ListLedgersCommand.ListLedgersFlags;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command for list all ledgers on the cluster.
 */
public class ListLedgersCommand extends BookieCommand<ListLedgersFlags> {

    static final Logger LOG = LoggerFactory.getLogger(ListLedgersCommand.class);

    private static final String NAME = "listledgers";
    private static final String DESC = "List all ledgers on the cluster (this may take a long time).";
    private static final String DEFAULT = "";

    private LedgerIdFormatter ledgerIdFormatter;

    public ListLedgersCommand() {
        this(new ListLedgersFlags());
    }

    public ListLedgersCommand(LedgerIdFormatter ledgerIdFormatter) {
        this(new ListLedgersFlags());
        this.ledgerIdFormatter = ledgerIdFormatter;
    }

    public ListLedgersCommand(ListLedgersFlags flags) {
        super(CliSpec.<ListLedgersFlags>newBuilder()
                  .withName(NAME)
                  .withDescription(DESC)
                  .withFlags(flags)
                  .build());
    }

    /**
     * Flags for ListLedgers command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class ListLedgersFlags extends CliFlags{

        @Parameter(names = {"-m", "--meta"}, description = "Print metadata")
        private boolean meta;

        @Parameter(names = { "-id", "--bookieid" }, description = "List ledgers residing in this bookie")
        private String bookieId;

        @Parameter(names = { "-l", "--ledgerIdFormatter" }, description = "Set ledger id formatter")
        private String ledgerIdFormatter = DEFAULT;
    }

    @Override
    public boolean apply(ServerConfiguration conf, ListLedgersFlags cmdFlags) {
        initLedgerFrommat(conf, cmdFlags);
        try {
            handler(conf, cmdFlags);
        } catch (UnknownHostException e) {
            System.err.println("Bookie id error");
            return false;
        } catch (MetadataException | ExecutionException e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }

        return true;
    }

    private void initLedgerFrommat(ServerConfiguration conf, ListLedgersFlags cmdFlags) {
        if (ledgerIdFormatter != null) {
            return;
        }
        if (!cmdFlags.ledgerIdFormatter.equals(DEFAULT)) {
            this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(cmdFlags.ledgerIdFormatter, conf);
        } else {
            this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(conf);
        }
    }

    public boolean handler(ServerConfiguration conf, ListLedgersFlags flags)
        throws UnknownHostException, MetadataException, ExecutionException {

        final BookieSocketAddress bookieAddress = StringUtils.isBlank(flags.bookieId) ? null :
                                                      new BookieSocketAddress(flags.bookieId);

        runFunctionWithLedgerManagerFactory(conf, mFactory -> {
            try (LedgerManager ledgerManager = mFactory.newLedgerManager()) {

                final AtomicInteger returnCode = new AtomicInteger(BKException.Code.OK);
                final CountDownLatch processDone = new CountDownLatch(1);

                BookkeeperInternalCallbacks.Processor<Long> ledgerProcessor = (ledgerId, cb) -> {
                    if (!flags.meta && (bookieAddress == null)) {
                        printLedgerMetadata(ledgerId, null, false);
                        cb.processResult(BKException.Code.OK, null, null);
                    } else {
                        ledgerManager.readLedgerMetadata(ledgerId).whenComplete((metadata, exception) -> {
                            if (exception == null) {
                                if ((bookieAddress == null)
                                        || BookKeeperAdmin.areEntriesOfLedgerStoredInTheBookie
                                               (ledgerId, bookieAddress, metadata.getValue())) {
                                    /*
                                     * the print method has to be in
                                     * synchronized scope, otherwise
                                     * output of printLedgerMetadata
                                     * could interleave since this
                                     * callback for different
                                     * ledgers can happen in
                                     * different threads.
                                     */
                                    synchronized (ListLedgersCommand.this) {
                                        printLedgerMetadata(ledgerId, metadata.getValue(), flags.meta);
                                    }
                                }
                                cb.processResult(BKException.Code.OK, null, null);
                            } else if (BKException.getExceptionCode(exception)
                                      == BKException.Code.NoSuchLedgerExistsException) {
                                cb.processResult(BKException.Code.OK, null, null);
                            } else {
                                LOG.error("Unable to read the ledger: {} information", ledgerId);
                                cb.processResult(BKException.getExceptionCode(exception), null, null);
                            }
                        });
                    }
                };

                ledgerManager.asyncProcessLedgers(ledgerProcessor, (rc, s, obj) -> {
                    returnCode.set(rc);
                    processDone.countDown();
                }, null, BKException.Code.OK, BKException.Code.ReadException);

                processDone.await();
                if (returnCode.get() != BKException.Code.OK) {
                    LOG.error("Received error return value while processing ledgers: {}", returnCode.get());
                    throw BKException.create(returnCode.get());
                }

            } catch (Exception ioe) {
                LOG.error("Received Exception while processing ledgers", ioe);
                throw new UncheckedExecutionException(ioe);
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
