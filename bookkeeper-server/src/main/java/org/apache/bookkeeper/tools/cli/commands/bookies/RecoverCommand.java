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
package org.apache.bookkeeper.tools.cli.commands.bookies;

import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithRegistrationManager;

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to ledger data recovery for failed bookie.
 */
public class RecoverCommand extends BookieCommand<RecoverCommand.RecoverFlags> {

    static final Logger LOG = LoggerFactory.getLogger(RecoverCommand.class);

    private static final String NAME = "recover";
    private static final String DESC = "Recover the ledger data for failed bookie";

    private static final long DEFAULT_ID = -1L;

    public RecoverCommand() {
        this(new RecoverFlags());
    }

    private RecoverCommand(RecoverFlags flags) {
        super(CliSpec.<RecoverFlags>newBuilder()
                  .withName(NAME)
                  .withDescription(DESC)
                  .withFlags(flags)
                  .build());
    }

    /**
     * Flags for recover command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class RecoverFlags extends CliFlags{

        @Parameter(names = { "-l", "--ledger" }, description = "Recover a specific ledger")
        private long ledger = DEFAULT_ID;

        @Parameter(names = { "-f", "--force" }, description = "Force recovery without confirmation")
        private boolean force;

        @Parameter(names = { "-q", "--query" }, description = "Query the ledgers that contain given bookies")
        private boolean query;

        @Parameter(names = { "-dr", "--drarun" }, description = "Printing the recovery plan w/o doing actual recovery")
        private boolean dryRun;

        @Parameter(names = {"-sk", "--skipopenledgers"}, description = "Skip recovering open ledgers")
        private boolean skipOpenLedgers;

        @Parameter(names = { "-d", "--deletecookie" }, description = "Delete cookie node for the bookie")
        private boolean deleteCookie;

        @Parameter(names = { "-bs", "--bokiesrc" }, description = "Bookie address")
        private String bookieAddress;
    }

    @Override
    public boolean apply(ServerConfiguration conf, RecoverFlags cmdFlags) {
        try {
            return recover(conf, cmdFlags);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private boolean recover(ServerConfiguration conf, RecoverFlags flags)
        throws IOException, BKException, InterruptedException, KeeperException {
        boolean query = flags.query;
        boolean dryrun = flags.dryRun;
        boolean force = flags.force;
        boolean skipOpenLedgers = flags.skipOpenLedgers;
        boolean removeCookies = !dryrun && flags.deleteCookie;

        Long ledgerId = flags.ledger;

        // Get bookies list
        final String[] bookieStrs = flags.bookieAddress.split(",");
        final Set<BookieSocketAddress> bookieAddrs = new HashSet<>();
        for (String bookieStr : bookieStrs) {
            final String[] bookieStrParts = bookieStr.split(":");
            if (bookieStrParts.length != 2) {
                System.err.println("BookieSrcs has invalid bookie address format (host:port expected) : "
                                   + bookieStr);
                return false;
            }
            bookieAddrs.add(new BookieSocketAddress(bookieStrParts[0],
                                                    Integer.parseInt(bookieStrParts[1])));
        }

        if (!force) {
            System.err.println("Bookies : " + bookieAddrs);
            if (!IOUtils.confirmPrompt("Are you sure to recover them : (Y/N)")) {
                System.err.println("Give up!");
                return false;
            }
        }

        LOG.info("Constructing admin");
        ClientConfiguration adminConf = new ClientConfiguration(conf);
        BookKeeperAdmin admin = new BookKeeperAdmin(adminConf);
        LOG.info("Construct admin : {}", admin);
        try {
            if (query) {
                return bkQuery(admin, bookieAddrs);
            }
            if (DEFAULT_ID != ledgerId) {
                return bkRecoveryLedger(admin, ledgerId, bookieAddrs, dryrun, skipOpenLedgers, removeCookies);
            }
            return bkRecovery(admin, bookieAddrs, dryrun, skipOpenLedgers, removeCookies);
        } finally {
            admin.close();
        }
    }

    private boolean bkQuery(BookKeeperAdmin bkAdmin, Set<BookieSocketAddress> bookieAddrs)
        throws InterruptedException, BKException {
        SortedMap<Long, LedgerMetadata> ledgersContainBookies =
            bkAdmin.getLedgersContainBookies(bookieAddrs);
        System.err.println("NOTE: Bookies in inspection list are marked with '*'.");
        for (Map.Entry<Long, LedgerMetadata> ledger : ledgersContainBookies.entrySet()) {
            System.out.println("ledger " + ledger.getKey() + " : " + ledger.getValue().getState());
            Map<Long, Integer> numBookiesToReplacePerEnsemble =
                inspectLedger(ledger.getValue(), bookieAddrs);
            System.out.print("summary: [");
            for (Map.Entry<Long, Integer> entry : numBookiesToReplacePerEnsemble.entrySet()) {
                System.out.print(entry.getKey() + "=" + entry.getValue() + ", ");
            }
            System.out.println("]");
            System.out.println();
        }
        System.err.println("Done");
        return true;
    }

    private Map<Long, Integer> inspectLedger(LedgerMetadata metadata, Set<BookieSocketAddress> bookiesToInspect) {
        Map<Long, Integer> numBookiesToReplacePerEnsemble = new TreeMap<Long, Integer>();
        for (Map.Entry<Long, ? extends List<BookieSocketAddress>> ensemble :
            metadata.getAllEnsembles().entrySet()) {
            List<BookieSocketAddress> bookieList = ensemble.getValue();
            System.out.print(ensemble.getKey() + ":\t");
            int numBookiesToReplace = 0;
            for (BookieSocketAddress bookie : bookieList) {
                System.out.print(bookie);
                if (bookiesToInspect.contains(bookie)) {
                    System.out.print("*");
                    ++numBookiesToReplace;
                } else {
                    System.out.print(" ");
                }
                System.out.print(" ");
            }
            System.out.println();
            numBookiesToReplacePerEnsemble.put(ensemble.getKey(), numBookiesToReplace);
        }
        return numBookiesToReplacePerEnsemble;
    }

    private boolean bkRecoveryLedger(BookKeeperAdmin bkAdmin,
                                 long lid,
                                 Set<BookieSocketAddress> bookieAddrs,
                                 boolean dryrun,
                                 boolean skipOpenLedgers,
                                 boolean removeCookies)
        throws InterruptedException, BKException {
        bkAdmin.recoverBookieData(lid, bookieAddrs, dryrun, skipOpenLedgers);
        if (removeCookies) {
            deleteCookies(bkAdmin.getConf(), bookieAddrs);
        }
        return true;
    }

    private void deleteCookies(ClientConfiguration conf,
                               Set<BookieSocketAddress> bookieAddrs) throws BKException {
        ServerConfiguration serverConf = new ServerConfiguration(conf);
        try {
            runFunctionWithRegistrationManager(serverConf, rm -> {
                try {
                    for (BookieSocketAddress addr : bookieAddrs) {
                        deleteCookie(rm, addr);
                    }
                } catch (Exception e) {
                    throw new UncheckedExecutionException(e);
                }
                return null;
            });
        } catch (Exception e) {
            Throwable cause = e;
            if (e instanceof UncheckedExecutionException) {
                cause = e.getCause();
            }
            if (cause instanceof BKException) {
                throw (BKException) cause;
            } else {
                BKException bke = new BKException.MetaStoreException();
                bke.initCause(bke);
                throw bke;
            }
        }

    }

    private void deleteCookie(RegistrationManager rm, BookieSocketAddress bookieSrc) throws BookieException {
        try {
            Versioned<Cookie> cookie = Cookie.readFromRegistrationManager(rm, bookieSrc);
            cookie.getValue().deleteFromRegistrationManager(rm, bookieSrc, cookie.getVersion());
        } catch (BookieException.CookieNotFoundException nne) {
            LOG.warn("No cookie to remove for {} : ", bookieSrc, nne);
        }
    }

    private boolean bkRecovery(BookKeeperAdmin bkAdmin,
                           Set<BookieSocketAddress> bookieAddrs,
                           boolean dryrun,
                           boolean skipOpenLedgers,
                           boolean removeCookies)
        throws InterruptedException, BKException {
        bkAdmin.recoverBookieData(bookieAddrs, dryrun, skipOpenLedgers);
        if (removeCookies) {
            deleteCookies(bkAdmin.getConf(), bookieAddrs);
        }
        return true;
    }
}
