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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithRegistrationManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.bookie.BookieException.UpgradeException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.HardLink;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application for upgrading the bookkeeper filesystem between versions.
 */
public class FileSystemUpgrade {
    private static final Logger LOG = LoggerFactory.getLogger(FileSystemUpgrade.class);

    static FilenameFilter bookieFilesFilter = new FilenameFilter() {
            private boolean containsIndexFiles(File dir, String name) {
                if (name.endsWith(".idx")) {
                    return true;
                }

                try {
                    Long.parseLong(name, 16);
                    File d = new File(dir, name);
                    if (d.isDirectory()) {
                        String[] files = d.list();
                        if (files != null) {
                            for (String f : files) {
                                if (containsIndexFiles(d, f)) {
                                    return true;
                                }
                            }
                        }
                    }
                } catch (NumberFormatException nfe) {
                    return false;
                }
                return false;
            }

            @Override
            public boolean accept(File dir, String name) {
                if (name.endsWith(".txn") || name.endsWith(".log")
                    || name.equals("lastId") || name.startsWith("lastMark")) {
                    return true;
                }
                return containsIndexFiles(dir, name);
            }
        };

    @VisibleForTesting
    public static List<File> getAllDirectories(ServerConfiguration conf) {
        List<File> dirs = new ArrayList<>();
        dirs.addAll(Lists.newArrayList(conf.getJournalDirs()));
        final File[] ledgerDirs = conf.getLedgerDirs();
        final File[] indexDirs = conf.getIndexDirs();
        if (indexDirs != null && indexDirs.length == ledgerDirs.length
                && !Arrays.asList(indexDirs).containsAll(Arrays.asList(ledgerDirs))) {
            dirs.addAll(Lists.newArrayList(indexDirs));
        }
        Collections.addAll(dirs, ledgerDirs);
        return dirs;
    }

    private static int detectPreviousVersion(File directory) throws IOException {
        String[] files = directory.list(bookieFilesFilter);
        File v2versionFile = new File(directory,
                BookKeeperConstants.VERSION_FILENAME);
        if ((files == null || files.length == 0) && !v2versionFile.exists()) { // no old data, so we're ok
            return Cookie.CURRENT_COOKIE_LAYOUT_VERSION;
        }

        if (!v2versionFile.exists()) {
            return 1;
        }
        try (Scanner s = new Scanner(v2versionFile, UTF_8.name())) {
            return s.nextInt();
        } catch (NoSuchElementException nse) {
            LOG.error("Couldn't parse version file " + v2versionFile, nse);
            throw new IOException("Couldn't parse version file", nse);
        } catch (IllegalStateException ise) {
            LOG.error("Error reading file " + v2versionFile, ise);
            throw new IOException("Error reading version file", ise);
        }
    }

    private static void linkIndexDirectories(File srcPath, File targetPath) throws IOException {
        String[] files = srcPath.list();
        if (files == null) {
            return;
        }
        for (String f : files) {
            if (f.endsWith(".idx")) { // this is an index dir, create the links
                if (!targetPath.mkdirs()) {
                    throw new IOException("Could not create target path [" + targetPath + "]");
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

        try {
            runFunctionWithRegistrationManager(conf, rm -> {
                try {
                    upgrade(conf, rm);
                } catch (UpgradeException e) {
                    throw new UncheckedExecutionException(e.getMessage(), e);
                }
                return null;
            });
        } catch (MetadataException e) {
            throw new UpgradeException(e);
        } catch (ExecutionException e) {
            throw new UpgradeException(e.getCause());
        }

        LOG.info("Done");
    }

    private static void upgrade(ServerConfiguration conf,
                                RegistrationManager rm) throws UpgradeException {
        try {
            Map<File, File> deferredMoves = new HashMap<File, File>();
            Cookie.Builder cookieBuilder = Cookie.generateCookie(conf);
            Cookie c = cookieBuilder.build();
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
                            @Override
                            public boolean accept(File dir, String name) {
                                return bookieFilesFilter.accept(dir, name)
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

            for (Map.Entry<File, File> e : deferredMoves.entrySet()) {
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
                c.writeToRegistrationManager(rm, conf, Version.NEW);
            } catch (BookieException ke) {
                LOG.error("Error writing cookie to registration manager");
                throw new BookieException.UpgradeException(ke);
            }
        } catch (IOException ioe) {
            throw new BookieException.UpgradeException(ioe);
        }
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
                    File[] files = d.listFiles(bookieFilesFilter);
                    if (files != null) {
                        for (File f : files) {
                            if (f.isDirectory()) {
                                FileUtils.deleteDirectory(f);
                            } else {
                                if (!f.delete()) {
                                    LOG.warn("Could not delete {}", f);
                                }
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

        try {
            runFunctionWithRegistrationManager(conf, rm -> {
                try {
                    rollback(conf, rm);
                } catch (UpgradeException e) {
                    throw new UncheckedExecutionException(e.getMessage(), e);
                }
                return null;
            });
        } catch (MetadataException e) {
            throw new UpgradeException(e);
        } catch (ExecutionException e) {
            throw new UpgradeException(e.getCause());
        }

        LOG.info("Done");
    }

    private static void rollback(ServerConfiguration conf,
                                 RegistrationManager rm)
            throws BookieException.UpgradeException {
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
            Versioned<Cookie> cookie = Cookie.readFromRegistrationManager(rm, conf);
            cookie.getValue().deleteFromRegistrationManager(rm, conf, cookie.getVersion());
        } catch (BookieException ke) {
            LOG.error("Error deleting cookie from Registration Manager");
            throw new BookieException.UpgradeException(ke);
        }
    }

    private static void printHelp(Options opts) {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("FileSystemUpgrade [options]", opts);
    }

    public static void main(String[] args) throws Exception {

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
