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
 *
 */
package org.apache.bookkeeper.benchmark;

import static java.nio.charset.StandardCharsets.UTF_8;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A benchmark that benchmarks the read throughput and latency.
 */
public class BenchReadThroughputLatency {
    static final Logger LOG = LoggerFactory.getLogger(BenchReadThroughputLatency.class);

    private static final Pattern LEDGER_PATTERN = Pattern.compile("L([0-9]+)$");

    private static final Comparator<String> ZK_LEDGER_COMPARE = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            try {
                Matcher m1 = LEDGER_PATTERN.matcher(o1);
                Matcher m2 = LEDGER_PATTERN.matcher(o2);
                if (m1.find() && m2.find()) {
                    return Integer.parseInt(m1.group(1))
                        - Integer.parseInt(m2.group(1));
                } else {
                    return o1.compareTo(o2);
                }
            } catch (Throwable t) {
                return o1.compareTo(o2);
            }
        }
    };

    private static void readLedger(ClientConfiguration conf, long ledgerId, byte[] passwd, int batchEntries) {
        LOG.info("Reading ledger {}", ledgerId);
        BookKeeper bk = null;
        long time = 0;
        long entriesRead = 0;
        long lastRead = -1;
        int nochange = 0;

        long absoluteLimit = 5000000;
        LedgerHandle lh = null;
        try {
            bk = new BookKeeper(conf);
            while (true) {
                lh = bk.openLedgerNoRecovery(ledgerId, BookKeeper.DigestType.CRC32,
                                             passwd);
                long lastConfirmed = Math.min(lh.getLastAddConfirmed(), absoluteLimit);
                if (lastConfirmed <= lastRead + 1) {
                    nochange++;
                    if (nochange == 10) {
                        break;
                    } else {
                        Thread.sleep(1000);
                        continue;
                    }
                } else {
                    nochange = 0;
                }
                long starttime = System.nanoTime();

                while (lastRead < lastConfirmed) {
                    long nextLimit = lastRead + 100000;
                    Enumeration<LedgerEntry> entries;
                    if (batchEntries <= 0) {
                        long readTo = Math.min(nextLimit, lastConfirmed);
                        entries = lh.readEntries(lastRead + 1, readTo);
                    } else {
                        entries = lh.batchReadEntries(lastRead + 1, batchEntries, -1);
                    }
                    while (entries.hasMoreElements()) {
                        LedgerEntry e = entries.nextElement();
                        entriesRead++;
                        lastRead = e.getEntryId();
                        if ((entriesRead % 10000) == 0) {
                            LOG.info("{} entries read from ledger {}", entriesRead, ledgerId);
                        }
                        e.getEntryBuffer().release();
                    }
                }
                long endtime = System.nanoTime();
                time += endtime - starttime;

                lh.close();
                lh = null;
                Thread.sleep(1000);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.error("Exception in reader", e);
        } finally {
            LOG.info("Read {} in {}ms", entriesRead, time / 1000 / 1000);

            try {
                if (lh != null) {
                    lh.close();
                }
                if (bk != null) {
                    bk.close();
                }
            } catch (Exception e) {
                LOG.error("Exception closing stuff", e);
            }
        }
    }

    private static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("BenchReadThroughputLatency <options>", options);
    }

    @SuppressWarnings("deprecation")
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("ledger", true, "Ledger to read. If empty, read all ledgers which come available. "
                          + " Cannot be used with -listen");
        //How to generate ledger node path.
        options.addOption("ledgerManagerType", true, "The ledger manager type. "
                + "The optional value: flat, hierarchical, legacyHierarchical, longHierarchical. Default: flat");
        options.addOption("listen", true, "Listen for creation of <arg> ledgers, and read each one fully");
        options.addOption("password", true, "Password used to access ledgers (default 'benchPasswd')");
        options.addOption("zookeeper", true, "Zookeeper ensemble, default \"localhost:2181\"");
        options.addOption("sockettimeout", true, "Socket timeout for bookkeeper client. In seconds. Default 5");
        options.addOption("useV2", false, "Whether use V2 protocol to read ledgers from the bookie server.");
        options.addOption("help", false, "This message");
        options.addOption("batchentries", true, "The batch read entries count. "
                + "If the value is greater than 0, uses batch read. Or uses the single read. Default 1000");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help")) {
            usage(options);
            System.exit(-1);
        }

        final String servers = cmd.getOptionValue("zookeeper", "localhost:2181");
        final byte[] passwd = cmd.getOptionValue("password", "benchPasswd").getBytes(UTF_8);
        final int sockTimeout = Integer.parseInt(cmd.getOptionValue("sockettimeout", "5"));
        final int batchentries = Integer.parseInt(cmd.getOptionValue("batchentries", "1000"));
        if (cmd.hasOption("ledger") && cmd.hasOption("listen")) {
            LOG.error("Cannot used -ledger and -listen together");
            usage(options);
            System.exit(-1);
        }

        final AtomicInteger ledger = new AtomicInteger(0);
        final AtomicInteger numLedgers = new AtomicInteger(0);
        if (cmd.hasOption("ledger")) {
            ledger.set(Integer.parseInt(cmd.getOptionValue("ledger")));
        } else if (cmd.hasOption("listen")) {
            numLedgers.set(Integer.parseInt(cmd.getOptionValue("listen")));
        } else {
            LOG.error("You must use -ledger or -listen");
            usage(options);
            System.exit(-1);
        }

        final CountDownLatch shutdownLatch = new CountDownLatch(1);

        String ledgerManagerType = cmd.getOptionValue("ledgerManagerType", "flat");
        String nodepath;
        if ("flat".equals(ledgerManagerType)) {
            nodepath = String.format("/ledgers/L%010d", ledger.get());
        } else if ("hierarchical".equals(ledgerManagerType)) {
            nodepath = String.format("/ledgers%s", StringUtils.getHybridHierarchicalLedgerPath(ledger.get()));
        } else if ("legacyHierarchical".equals(ledgerManagerType)) {
            nodepath = String.format("/ledgers%s", StringUtils.getShortHierarchicalLedgerPath(ledger.get()));
        } else if ("longHierarchical".equals(ledgerManagerType)) {
            nodepath = String.format("/ledgers%s", StringUtils.getLongHierarchicalLedgerPath(ledger.get()));
        } else {
            LOG.warn("Unknown ledger manager type: {}, use flat as the value", ledgerManagerType);
            nodepath = String.format("/ledgers/L%010d", ledger.get());
        }

        final ClientConfiguration conf = new ClientConfiguration();
        conf.setReadTimeout(sockTimeout).setZkServers(servers);

        if (cmd.hasOption("useV2")) {
            conf.setUseV2WireProtocol(true);
        }

        try (ZooKeeperClient zk = ZooKeeperClient.newBuilder()
                .connectString(servers)
                .sessionTimeoutMs(3000)
                .build()) {
            final Set<String> processedLedgers = new HashSet<String>();
            zk.register(new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        try {
                            if (event.getType() == Event.EventType.NodeCreated
                                       && event.getPath().equals(nodepath)) {
                                readLedger(conf, ledger.get(), passwd, batchentries);
                                shutdownLatch.countDown();
                            } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
                                if (numLedgers.get() < 0) {
                                    return;
                                }
                                List<String> children = zk.getChildren("/ledgers", true);
                                List<String> ledgers = new ArrayList<String>();
                                for (String child : children) {
                                    if (LEDGER_PATTERN.matcher(child).find()) {
                                        ledgers.add(child);
                                    }
                                }
                                for (String ledger : ledgers) {
                                    synchronized (processedLedgers) {
                                        if (processedLedgers.contains(ledger)) {
                                            continue;
                                        }
                                        final Matcher m = LEDGER_PATTERN.matcher(ledger);
                                        if (m.find()) {
                                            int ledgersLeft = numLedgers.decrementAndGet();
                                            final Long ledgerId = Long.valueOf(m.group(1));
                                            processedLedgers.add(ledger);
                                            Thread t = new Thread() {
                                                @Override
                                                public void run() {
                                                    readLedger(conf, ledgerId, passwd, batchentries);
                                                }
                                            };
                                            t.start();
                                            if (ledgersLeft <= 0) {
                                                shutdownLatch.countDown();
                                            }
                                        } else {
                                            LOG.error("Cant file ledger id in {}", ledger);
                                        }
                                    }
                                }
                            } else {
                                LOG.warn("Unknown event {}", event);
                            }
                        } catch (Exception e) {
                            LOG.error("Exception in watcher", e);
                        }
                    }
                });

            if (ledger.get() != 0) {
                if (zk.exists(nodepath, true) != null) {
                    readLedger(conf, ledger.get(), passwd, batchentries);
                    shutdownLatch.countDown();
                } else {
                    LOG.info("Watching for creation of" + nodepath);
                }
            } else {
                zk.getChildren("/ledgers", true);
            }
            shutdownLatch.await();
            LOG.info("Shutting down");
        }
    }
}
