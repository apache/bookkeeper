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

package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.HardLink;

import org.apache.commons.io.FileUtils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.NoSuchElementException;

import java.net.MalformedURLException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.commons.configuration.ConfigurationException;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;

/**
 * Application for upgrading the bookkeeper filesystem
 * between versions
 */
public class FileSystemUpgrade {
    static Logger LOG = LoggerFactory.getLogger(FileSystemUpgrade.class);

    static FilenameFilter BOOKIE_FILES_FILTER = new FilenameFilter() {
            private boolean containsIndexFiles(File dir, String name) {
                if (name.endsWith(".idx")) {
                    return true;
                }

                try {
                    Long.parseLong(name, 16);
                    File d = new File(dir, name);
                    if (d.isDirectory()) {
                        String[] files = d.list();
                        for (String f : files) {
                            if (containsIndexFiles(d, f)) {
                                return true;
                            }
                        }
                    }
                } catch (NumberFormatException nfe) {
                    return false;
                }
                return false;
            }

            public boolean accept(File dir, String name) {
                if (name.endsWith(".txn") || name.endsWith(".log")
                    || name.equals("lastId") || name.equals("lastMark")) {
                    return true;
                }
                if (containsIndexFiles(dir, name)) {
                    return true;
                }
                return false;
            }
        };

    private static List<File> getAllDirectories(ServerConfiguration conf) {
        List<File> dirs = new ArrayList<File>();
        dirs.add(conf.getJournalDir());
        for (File d: conf.getLedgerDirs()) {
            dirs.add(d);
        }
        return dirs;
    }

    private static int detectPreviousVersion(File directory) throws IOException {
        String[] files = directory.list(BOOKIE_FILES_FILTER);
        File v2versionFile = new File(directory,
                BookKeeperConstants.VERSION_FILENAME);
        if (files.length == 0 && !v2versionFile.exists()) { // no old data, so we're ok
            return Cookie.CURRENT_COOKIE_LAYOUT_VERSION;
        }

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

    private static ZooKeeper newZookeeper(final ServerConfiguration conf)
            throws BookieException.UpgradeException {
        try {
            final CountDownLatch latch = new CountDownLatch(1);
            ZooKeeper zk = new ZooKeeper(conf.getZkServers(), conf.getZkTimeout(),
                    new Watcher() {
                        @Override
                        public void process(WatchedEvent event) {
                            // handle session disconnects and expires
                            if (event.getState().equals(Watcher.Event.KeeperState.SyncConnected)) {
                                latch.countDown();
                            }
                        }
                    });
            if (!latch.await(conf.getZkTimeout()*2, TimeUnit.MILLISECONDS)) {
                zk.close();
                throw new BookieException.UpgradeException("Couldn't connect to zookeeper");
            }
            return zk;
        } catch (InterruptedException ie) {
            throw new BookieException.UpgradeException(ie);
        } catch (IOException ioe) {
            throw new BookieException.UpgradeException(ioe);
        }
    }

    private static void linkIndexDirectories(File srcPath, File targetPath) throws IOException {
        String[] files = srcPath.list();

        for (String f : files) {
            if (f.endsWith(".idx")) { // this is an index dir, create the links
                if (!targetPath.mkdirs()) {
                    throw new IOException("Could not create target path ["+targetPath+"]");
                }
                HardLink.createHardLinkMult(srcPath, files, targetPath);
                return;
            }
            File newSrcPath = new File(srcPath, f);
            if (newSrcPath.isDirectory()) {
                try {
                    Long.parseLong(f, 16);
                    linkIndexDirectories(newSrcPath, new File(targetPath, f));
                } catch (NumberFormatException nfe) {
                    // filename does not parse to a hex Long, so
                    // it will not contain idx files. Ignoring
                }
            }
        }
    }

    public static void upgrade(ServerConfiguration conf)
            throws BookieException.UpgradeException, InterruptedException {
        LOG.info("Upgrading...");

        ZooKeeper zk = newZookeeper(conf);
        try {
            Map<File,File> deferredMoves = new HashMap<File, File>();
            Cookie c = Cookie.generateCookie(conf);
            for (File d : getAllDirectories(conf)) {
                LOG.info("Upgrading {}", d);
                int version = detectPreviousVersion(d);
                if (version == Cookie.CURRENT_COOKIE_LAYOUT_VERSION) {
                    LOG.info("Directory is current, no need to upgrade");
                    continue;
                }
                try {
                    File curDir = new File(d, BookKeeperConstants.CURRENT_DIR);
                    File tmpDir = new File(d, "upgradeTmp." + System.nanoTime());
                    deferredMoves.put(curDir, tmpDir);
                    if (!tmpDir.mkdirs()) {
                        throw new BookieException.UpgradeException("Could not create temporary directory " + tmpDir);
                    }
                    c.writeToDirectory(tmpDir);

                    String[] files = d.list(new FilenameFilter() {
                            public boolean accept(File dir, String name) {
                                return BOOKIE_FILES_FILTER.accept(dir, name)
                                    && !(new File(dir, name).isDirectory());
                            }
                        });
                    HardLink.createHardLinkMult(d, files, tmpDir);

                    linkIndexDirectories(d, tmpDir);
                } catch (IOException ioe) {
                    LOG.error("Error upgrading {}", d);
                    throw new BookieException.UpgradeException(ioe);
                }
            }

            for (Map.Entry<File,File> e : deferredMoves.entrySet()) {
                try {
                    FileUtils.moveDirectory(e.getValue(), e.getKey());
                } catch (IOException ioe) {
                    String err = String.format("Error moving upgraded directories into place %s -> %s ",
                                               e.getValue(), e.getKey());
                    LOG.error(err, ioe);
                    throw new BookieException.UpgradeException(ioe);
                }
            }

            if (deferredMoves.isEmpty()) {
                return;
            }

            try {
                c.writeToZooKeeper(zk, conf);
            } catch (KeeperException ke) {
                LOG.error("Error writing cookie to zookeeper");
                throw new BookieException.UpgradeException(ke);
            }
        } catch (IOException ioe) {
            throw new BookieException.UpgradeException(ioe);
        } finally {
            zk.close();
        }
        LOG.info("Done");
    }

    public static void finalizeUpgrade(ServerConfiguration conf)
            throws BookieException.UpgradeException, InterruptedException {
        LOG.info("Finalizing upgrade...");
        // verify that upgrade is correct
        for (File d : getAllDirectories(conf)) {
            LOG.info("Finalizing {}", d);
            try {
                int version = detectPreviousVersion(d);
                if (version < 3) {
                    if (version == 2) {
                        File v2versionFile = new File(d,
                                BookKeeperConstants.VERSION_FILENAME);
                        if (!v2versionFile.delete()) {
                            LOG.warn("Could not delete old version file {}", v2versionFile);
                        }
                    }
                    File[] files = d.listFiles(BOOKIE_FILES_FILTER);
                    for (File f : files) {
                        if (f.isDirectory()) {
                            FileUtils.deleteDirectory(f);
                        } else{
                            if (!f.delete()) {
                                LOG.warn("Could not delete {}", f);
                            }
                        }
                    }
                }
            } catch (IOException ioe) {
                LOG.error("Error finalizing {}", d);
                throw new BookieException.UpgradeException(ioe);
            }
        }
        // noop at the moment
        LOG.info("Done");
    }

    public static void rollback(ServerConfiguration conf)
            throws BookieException.UpgradeException, InterruptedException {
        LOG.info("Rolling back upgrade...");
        ZooKeeper zk = newZookeeper(conf);
        try {
            for (File d : getAllDirectories(conf)) {
                LOG.info("Rolling back {}", d);
                try {
                    // ensure there is a previous version before rollback
                    int version = detectPreviousVersion(d);

                    if (version <= Cookie.CURRENT_COOKIE_LAYOUT_VERSION) {
                        File curDir = new File(d,
                                BookKeeperConstants.CURRENT_DIR);
                        FileUtils.deleteDirectory(curDir);
                    } else {
                        throw new BookieException.UpgradeException(
                                "Cannot rollback as previous data does not exist");
                    }
                } catch (IOException ioe) {
                    LOG.error("Error rolling back {}", d);
                    throw new BookieException.UpgradeException(ioe);
                }
            }
            try {
                Cookie c = Cookie.readFromZooKeeper(zk, conf);
                c.deleteFromZooKeeper(zk, conf);
            } catch (KeeperException ke) {
                LOG.error("Error deleting cookie from ZooKeeper");
                throw new BookieException.UpgradeException(ke);
            } catch (IOException ioe) {
                LOG.error("I/O Error deleting cookie from ZooKeeper");
                throw new BookieException.UpgradeException(ioe);
            }
        } finally {
            zk.close();
        }
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
        org.apache.log4j.Logger.getLogger(FileSystemUpgrade.class).setLevel(
                org.apache.log4j.Level.INFO);

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
