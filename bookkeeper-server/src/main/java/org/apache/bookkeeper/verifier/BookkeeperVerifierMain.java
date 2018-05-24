/**
 *
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

package org.apache.bookkeeper.verifier;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * Performs a configurable IO stream against a bookkeeper client while
 * validating results.
 */
public class BookkeeperVerifierMain {

    private static void printHelpAndExit(Options options, String header, int code) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(
                "BookkeeperVerifierMain",
                header,
                options, "", true);
        System.exit(code);
    }

    public static void main(String[] args) throws Exception {

        Options options = new Options();
        options.addOption(
                "ledger_path", true, "Hostname or IP of bookie to benchmark");
        options.addOption(
                "zookeeper",
                true,
                "Zookeeper ensemble, (default \"localhost:2181\")");
        options.addOption(
                "ensemble_size", true, "Bookkeeper client ensemble size");
        options.addOption(
                "write_quorum", true, "Bookkeeper client write quorum size");
        options.addOption("ack_quorum", true, "Bookkeeper client ack quorum size");
        options.addOption("duration", true, "Run duration in seconds");
        options.addOption("drain_timeout", true, "Seconds to wait for in progress ops to end");
        options.addOption(
                "target_concurrent_ledgers",
                true,
                "target number ledgers to write to concurrently");
        options.addOption(
                "target_concurrent_writes",
                true,
                "target number of concurrent writes per ledger");
        options.addOption(
                "target_write_group",
                true,
                "target number of entries to write at a time");
        options.addOption(
                "target_read_group",
                true,
                "target number of entries to read at a time");
        options.addOption("target_ledgers", true, "Target number of ledgers");
        options.addOption("target_ledger_size", true, "Target size per ledger");
        options.addOption("target_entry_size", true, "Target size per entry");
        options.addOption(
                "target_concurrent_reads", true, "Number of reads to maintain");
        options.addOption(
                "cold_to_hot_ratio", true, "Ratio of reads on open ledgers");
        options.addOption("help", false, "Print this help message");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            printHelpAndExit(options, "Unable to parse command line", 1);
        }

        if (cmd.hasOption("help")) {
            printHelpAndExit(options, "Help:", 0);
        }

        String ledgerPath = cmd.getOptionValue("ledger_path", "/ledgers");
        String zkString = cmd.getOptionValue("zookeeper", "localhost:2181");
        int ensembleSize = 0;
        int writeQuorum = 0;
        int ackQuorum = 0;
        int duration = 0;
        int drainTimeout = 0;
        int targetConcurrentLedgers = 0;
        int targetConcurrentWrites = 0;
        int targetWriteGroup = 0;
        int targetReadGroup = 0;
        int targetLedgers = 0;
        long targetLedgerSize = 0;
        int targetEntrySize = 0;
        int targetConcurrentReads = 0;
        double coldToHotRatio = 0;

        try {
            ensembleSize = Integer.parseInt(cmd.getOptionValue("ensemble_size", "3"));
            writeQuorum = Integer.parseInt(cmd.getOptionValue("write_quorum", "3"));
            ackQuorum = Integer.parseInt(cmd.getOptionValue("ack_quorum", "2"));
            duration = Integer.parseInt(cmd.getOptionValue("duration", "600"));
            drainTimeout = Integer.parseInt(cmd.getOptionValue("drain_timeout", "10"));
            targetConcurrentLedgers =
                    Integer.parseInt(cmd.getOptionValue("target_concurrent_ledgers", "4"));
            targetConcurrentWrites =
                    Integer.parseInt(cmd.getOptionValue("target_concurrent_writes", "12"));
            targetWriteGroup =
                    Integer.parseInt(cmd.getOptionValue("target_write_group", "4"));
            targetReadGroup =
                    Integer.parseInt(cmd.getOptionValue("target_read_group", "4"));
            targetLedgers = Integer.parseInt(cmd.getOptionValue("target_ledgers", "32"));
            targetLedgerSize = Long.parseLong(cmd.getOptionValue(
                    "target_ledger_size",
                    "33554432"));
            targetEntrySize = Integer.parseInt(cmd.getOptionValue(
                    "target_entry_size",
                    "16384"));
            targetConcurrentReads = Integer.parseInt(cmd.getOptionValue(
                    "target_concurrent_reads",
                    "16"));
            coldToHotRatio = Double.parseDouble(
                    cmd.getOptionValue("cold_to_hot_ratio", "0.5"));
        } catch (NumberFormatException e) {
            printHelpAndExit(options, "Invalid argument", 0);
        }

        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri("zk://" + zkString + ledgerPath);
        BookKeeper bkclient = new BookKeeper(conf);

        BookkeeperVerifier verifier = new BookkeeperVerifier(
                new DirectBookkeeperDriver(bkclient),
                ensembleSize,
                writeQuorum,
                ackQuorum,
                duration,
                drainTimeout,
                targetConcurrentLedgers,
                targetConcurrentWrites,
                targetWriteGroup,
                targetReadGroup,
                targetLedgers,
                targetLedgerSize,
                targetEntrySize,
                targetConcurrentReads,
                coldToHotRatio);
        try {
            verifier.run();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            bkclient.close();
        }
    }
}
