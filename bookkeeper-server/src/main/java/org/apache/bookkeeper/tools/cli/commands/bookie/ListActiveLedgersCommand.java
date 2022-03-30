/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.tools.cli.commands.bookie;

import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithLedgerManagerFactory;

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.EntryLogMetadata;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.bookie.ReadOnlyEntryLogger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.tools.cli.commands.bookie.ListActiveLedgersCommand.ActiveLedgerFlags;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 *  List active(exist in metadata storage) ledgers in a entry log file.
 *
 **/
@SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
public class ListActiveLedgersCommand extends BookieCommand<ActiveLedgerFlags>{
    private static final Logger LOG = LoggerFactory.getLogger(ListActiveLedgersCommand.class);
    private static final String NAME = "active ledger";
    private static final String DESC = "Retrieve bookie active ledger info.";
    private static final long  DEFAULT_TIME_OUT = 1000;
    private static final long  DEFAULT_LOG_ID = 0;
    private static final String DEFAULT_LEDGER_ID_FORMATTER = "";
    private LedgerIdFormatter ledgerIdFormatter;

  public ListActiveLedgersCommand(){
    this(new ActiveLedgerFlags());
  }
    public ListActiveLedgersCommand(LedgerIdFormatter ledgerIdFormatter){
        this(new ActiveLedgerFlags());
        this.ledgerIdFormatter = ledgerIdFormatter;
    }

    public ListActiveLedgersCommand(ActiveLedgerFlags ledgerFlags){
        super(CliSpec.<ActiveLedgerFlags>newBuilder().
                      withName(NAME).
                      withDescription(DESC).
                      withFlags(ledgerFlags).
                      build());
    }

    /**
     * Flags for active ledger  command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class ActiveLedgerFlags extends CliFlags {

        @Parameter(names = { "-l", "--logid" }, description = "Entry log file id")
        private long logId = DEFAULT_LOG_ID;
        @Parameter(names = { "-t", "--timeout" }, description = "Read timeout(ms)")
        private long timeout = DEFAULT_TIME_OUT;
        @Parameter(names = { "-f", "--ledgerIdFormatter" }, description = "Ledger id formatter")
        private String ledgerIdFormatter = DEFAULT_LEDGER_ID_FORMATTER;
    }
    @Override
    public boolean apply(ServerConfiguration bkConf, ActiveLedgerFlags cmdFlags){
      initLedgerFormatter(bkConf, cmdFlags);
        try {
          handler(bkConf, cmdFlags);
        } catch (MetadataException | ExecutionException e) {
          throw new UncheckedExecutionException(e.getMessage(), e);
        }
        return true;
    }

    private void initLedgerFormatter(ServerConfiguration conf, ActiveLedgerFlags cmdFlags) {
      if (!cmdFlags.ledgerIdFormatter.equals(DEFAULT_LEDGER_ID_FORMATTER)) {
        this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(cmdFlags.ledgerIdFormatter, conf);
      } else if (ledgerIdFormatter == null){
        this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(conf);
      }
    }

    public void handler(ServerConfiguration bkConf, ActiveLedgerFlags cmdFlags)
      throws ExecutionException, MetadataException {
      runFunctionWithLedgerManagerFactory(bkConf, mFactory -> {
        try (LedgerManager ledgerManager = mFactory.newLedgerManager()) {
          Set<Long> activeLedgersOnMetadata = new HashSet<Long>();
          BookkeeperInternalCallbacks.Processor<Long> ledgerProcessor = (ledger, cb)->{
            activeLedgersOnMetadata.add(ledger);
            cb.processResult(BKException.Code.OK, null, null);
          };
          CountDownLatch done = new CountDownLatch(1);
          AtomicInteger resultCode = new AtomicInteger(BKException.Code.OK);
          VoidCallback endCallback = (rs, s, obj)->{
            resultCode.set(rs);
            done.countDown();
          };
          ledgerManager.asyncProcessLedgers(ledgerProcessor, endCallback, null,
            BKException.Code.OK, BKException.Code.ReadException);
          if (done.await(cmdFlags.timeout, TimeUnit.MILLISECONDS)){
            if  (resultCode.get() == BKException.Code.OK) {
              EntryLogger entryLogger = new ReadOnlyEntryLogger(bkConf);
              EntryLogMetadata entryLogMetadata = entryLogger.getEntryLogMetadata(cmdFlags.logId);
              List<Long> ledgersOnEntryLog = entryLogMetadata.getLedgersMap().keys();
              if (ledgersOnEntryLog.size() == 0) {
                LOG.info("Ledgers on log file {} is empty", cmdFlags.logId);
              }
              List<Long> activeLedgersOnEntryLog = new ArrayList<Long>(ledgersOnEntryLog.size());
              for (long ledger : ledgersOnEntryLog) {
                if (activeLedgersOnMetadata.contains(ledger)) {
                  activeLedgersOnEntryLog.add(ledger);
                }
              }
              printActiveLedgerOnEntryLog(cmdFlags.logId, activeLedgersOnEntryLog);
            } else {
              LOG.info("Read active ledgers id from metadata store,fail code {}", resultCode.get());
              throw BKException.create(resultCode.get());
            }
          } else {
            LOG.info("Read active ledgers id from metadata store timeout");
          }
        } catch (BKException | InterruptedException | IOException e){
          LOG.error("Received Exception while processing ledgers", e);
          throw new UncheckedExecutionException(e);
        }
        return null;
      });
    }

    public void printActiveLedgerOnEntryLog(long logId, List<Long> activeLedgers){
      if (activeLedgers.size() == 0){
        LOG.info("No active ledgers on log file " + logId);
      } else {
        LOG.info("Active ledgers on entry log " + logId + " as follow:");
      }
      Collections.sort(activeLedgers);
      for (long a:activeLedgers){
        LOG.info(ledgerIdFormatter.formatLedgerId(a) + " ");
      }
    }
}
