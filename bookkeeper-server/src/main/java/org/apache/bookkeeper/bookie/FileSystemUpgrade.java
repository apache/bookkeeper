/*
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

package org.apache.bookkeeper.bookie;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Scanner;
import java.util.NoSuchElementException;

import java.net.MalformedURLException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.commons.configuration.ConfigurationException;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Application for upgrading the bookkeeper filesystem
 * between versions
 */
public class FileSystemUpgrade {
    static Logger LOG = LoggerFactory.getLogger(FileSystemUpgrade.class);

    private int detectPreviousVersion(File directory) throws IOException {
        final AtomicBoolean oldDataExists = new AtomicBoolean(false);
        directory.list(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    if (name.endsWith(".txn") || name.endsWith(".idx") || name.endsWith(".log")
                        || name.equals(Bookie.VERSION_FILENAME)) {
                        oldDataExists.set(true);
                    }
                    return true;
                }
            });
        if (!oldDataExists.get()) { // no old data, so we're ok
            return Bookie.CURRENT_DIRECTORY_LAYOUT_VERSION;
        }
        File v2versionFile = new File(directory, Bookie.VERSION_FILENAME);
        if (!v2versionFile.exists()) {
            return 1;
        }
        Scanner s = new Scanner(v2versionFile);
        try {
            return s.nextInt();
        } catch (NoSuchElementException nse) {
            LOG.error("Couldn't parse version file " + v2versionFile , nse);
            throw new IOException("Couldn't parse version file", nse);
        } catch (IllegalStateException ise) {
            LOG.error("Error reading file " + v2versionFile, ise);
            throw new IOException("Error reading version file", ise);
        } finally {
            s.close();
        }
    }

    public static void upgrade(ServerConfiguration conf) {
        LOG.info("Upgrading...");
        // noop at the moment
        LOG.info("Done");
    }

    public static void finalizeUpgrade(ServerConfiguration conf) {
        LOG.info("Finalizing upgrade...");
        // noop at the moment
        LOG.info("Done");
    }

    public static void rollback(ServerConfiguration conf) {
        LOG.info("Rolling back upgrade...");
        // noop at the moment
        LOG.info("Done");
    }

    private static void printHelp(Options opts) {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("FileSystemUpgrade [options]", opts);
    }

    public static void main(String[] args) throws Exception {
        org.apache.log4j.Logger root = org.apache.log4j.Logger.getRootLogger();
        root.addAppender(new org.apache.log4j.ConsoleAppender(
                                 new org.apache.log4j.PatternLayout("%-5p [%t]: %m%n")));
        root.setLevel(org.apache.log4j.Level.ERROR);
        root.getLogger(FileSystemUpgrade.class).setLevel(org.apache.log4j.Level.INFO);

        final Options opts = new Options();
        opts.addOption("c", "conf", true, "Configuration for Bookie");
        opts.addOption("u", "upgrade", false, "Upgrade bookie directories");
        opts.addOption("f", "finalize", false, "Finalize upgrade");
        opts.addOption("r", "rollback", false, "Rollback upgrade");
        opts.addOption("h", "help", false, "Print help message");

        BasicParser parser = new BasicParser();
        CommandLine cmdLine = parser.parse(opts, args);
        if (cmdLine.hasOption("h")) {
            printHelp(opts);
            return;
        }

        if (!cmdLine.hasOption("c")) {
            String err = "Cannot upgrade without configuration";
            LOG.error(err);
            printHelp(opts);
            throw new IllegalArgumentException(err);
        }

        String confFile = cmdLine.getOptionValue("c");
        ServerConfiguration conf = new ServerConfiguration();
        try {
            conf.loadConf(new File(confFile).toURI().toURL());
        } catch (MalformedURLException mue) {
            LOG.error("Could not open configuration file " + confFile, mue);
            throw new IllegalArgumentException();
        } catch (ConfigurationException ce) {
            LOG.error("Invalid configuration file " + confFile, ce);
            throw new IllegalArgumentException();
        }

        if (cmdLine.hasOption("u")) {
            upgrade(conf);
        } else if (cmdLine.hasOption("r")) {
            rollback(conf);
        } else if (cmdLine.hasOption("f")) {
            finalizeUpgrade(conf);
        } else {
            String err = "Must specify -upgrade, -finalize or -rollback";
            LOG.error(err);
            printHelp(opts);
            throw new IllegalArgumentException(err);
        }
    }
}