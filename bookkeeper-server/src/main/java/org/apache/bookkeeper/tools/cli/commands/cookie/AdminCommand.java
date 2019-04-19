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
package org.apache.bookkeeper.tools.cli.commands.cookie;

import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithMetadataBookieDriver;
import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithRegistrationManager;

import com.beust.jcommander.Parameter;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to update cookie.
 */
public class AdminCommand extends BookieCommand<AdminCommand.AdminFlags> {

    static final Logger LOG = LoggerFactory.getLogger(AdminCommand.class);

    private static final String NAME = "admin";
    private static final String DESC = "Command to update cookie";

    private File[] journalDirectories;
    private File[] ledgerDirectories;
    private File[] indexDirectories;

    public AdminCommand() {
        this(new AdminFlags());
    }

    private AdminCommand(AdminFlags flags) {
        super(CliSpec.<AdminFlags>newBuilder().withName(NAME).withDescription(DESC).withFlags(flags).build());
    }

    /**
     * Flags for admin command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class AdminFlags extends CliFlags {

        @Parameter(names = { "-host",
            "--hostname" }, description = "Expects config useHostNameAsBookieID=true as the option value")
        private boolean hostname;

        @Parameter(names = { "-p", "-ip" },
            description = "Expects config useHostNameAsBookieID=false as the option value")
        private boolean ip;

        @Parameter(names = { "-e", "--expandstorage" }, description = "Add new empty ledger/index directories")
        private boolean expandstorage;

        @Parameter(names = { "-l", "--list" }, description = "List paths of all the cookies present locally and on "
                                                             + "zooKeeper")
        private boolean list;

        @Parameter(names = { "-d", "--delete" }, description = "Delete cookie both locally and in zooKeeper")
        private boolean delete;

        @Parameter(names = {"-f", "--force"}, description = "Force delete cookie")
        private boolean force;

    }

    @Override
    public boolean apply(ServerConfiguration conf, AdminFlags cmdFlags) {
        initDirectory(conf);
        try {
            return update(conf, cmdFlags);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private void initDirectory(ServerConfiguration bkConf) {
        this.journalDirectories = Bookie.getCurrentDirectories(bkConf.getJournalDirs());
        this.ledgerDirectories = Bookie.getCurrentDirectories(bkConf.getLedgerDirs());
        if (null == bkConf.getIndexDirs()) {
            this.indexDirectories = this.ledgerDirectories;
        } else {
            this.indexDirectories = Bookie.getCurrentDirectories(bkConf.getIndexDirs());
        }
    }

    private boolean update(ServerConfiguration conf, AdminFlags flags) throws Exception {
        boolean useHostName = flags.hostname;
        if (flags.hostname || flags.ip) {
            if (!conf.getUseHostNameAsBookieID() && useHostName) {
                LOG.error("Expects configuration useHostNameAsBookieID=true as the option value");
                return false;
            } else if (conf.getUseHostNameAsBookieID() && !useHostName) {
                LOG.error("Expects configuration useHostNameAsBookieID=false as the option value");
                return false;
            }
            return updateBookieIdInCookie(conf, flags.hostname);
        } else if (flags.expandstorage) {
            conf.setAllowStorageExpansion(true);
            return expandStorage(conf);
        } else if (flags.list) {
            return listOrDeleteCookies(conf, false, false);
        } else if (flags.delete) {
            return listOrDeleteCookies(conf, true, flags.force);
        } else {
            LOG.error("Invalid command !");
            usage();
            return false;
        }
    }

    private boolean updateBookieIdInCookie(ServerConfiguration bkConf, final boolean useHostname)
        throws Exception {
        return runFunctionWithRegistrationManager(bkConf, rm -> {
            try {
                ServerConfiguration conf = new ServerConfiguration(bkConf);
                String newBookieId = Bookie.getBookieAddress(conf).toString();
                // read oldcookie
                Versioned<Cookie> oldCookie = null;
                try {
                    conf.setUseHostNameAsBookieID(!useHostname);
                    oldCookie = Cookie.readFromRegistrationManager(rm, conf);
                } catch (BookieException.CookieNotFoundException nne) {
                    LOG.error("Either cookie already updated with UseHostNameAsBookieID={} or no cookie exists!",
                              useHostname, nne);
                    return false;
                }
                Cookie newCookie = Cookie.newBuilder(oldCookie.getValue()).setBookieHost(newBookieId).build();

                boolean hasCookieUpdatedInDirs = verifyCookie(newCookie, journalDirectories[0]);
                for (File dir : ledgerDirectories) {
                    hasCookieUpdatedInDirs &= verifyCookie(newCookie, dir);
                }
                if (indexDirectories != ledgerDirectories) {
                    for (File dir : indexDirectories) {
                        hasCookieUpdatedInDirs &= verifyCookie(newCookie, dir);
                    }
                }

                if (hasCookieUpdatedInDirs) {
                    try {
                        conf.setUseHostNameAsBookieID(useHostname);
                        Cookie.readFromRegistrationManager(rm, conf);
                        // since newcookie exists, just do cleanup of oldcookie and return
                        conf.setUseHostNameAsBookieID(!useHostname);
                        oldCookie.getValue().deleteFromRegistrationManager(rm, conf, oldCookie.getVersion());
                        return true;
                    } catch (BookieException.CookieNotFoundException nne) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Ignoring, cookie will be written to zookeeper");
                        }
                    }
                } else {
                    // writes newcookie to local dirs
                    for (File journalDirectory : journalDirectories) {
                        newCookie.writeToDirectory(journalDirectory);
                        LOG.info("Updated cookie file present in journalDirectory {}", journalDirectory);
                    }
                    for (File dir : ledgerDirectories) {
                        newCookie.writeToDirectory(dir);
                    }
                    LOG.info("Updated cookie file present in ledgerDirectories {}", (Object) ledgerDirectories);
                    if (ledgerDirectories != indexDirectories) {
                        for (File dir : indexDirectories) {
                            newCookie.writeToDirectory(dir);
                        }
                        LOG.info("Updated cookie file present in indexDirectories {}", (Object) indexDirectories);
                    }
                }
                // writes newcookie to zookeeper
                conf.setUseHostNameAsBookieID(useHostname);
                newCookie.writeToRegistrationManager(rm, conf, Version.NEW);

                // delete oldcookie
                conf.setUseHostNameAsBookieID(!useHostname);
                oldCookie.getValue().deleteFromRegistrationManager(rm, conf, oldCookie.getVersion());
                return true;
            } catch (IOException | BookieException ioe) {
                LOG.error("IOException during cookie updation!", ioe);
                return false;
            }
        });
    }

    private boolean verifyCookie(Cookie oldCookie, File dir) throws IOException {
        try {
            Cookie cookie = Cookie.readFromDirectory(dir);
            cookie.verify(oldCookie);
        } catch (BookieException.InvalidCookieException e) {
            return false;
        }
        return true;
    }

    private boolean expandStorage(ServerConfiguration bkConf) throws Exception {
        return runFunctionWithMetadataBookieDriver(bkConf, driver -> {
            List<File> allLedgerDirs = Lists.newArrayList();
            allLedgerDirs.addAll(Arrays.asList(ledgerDirectories));
            if (indexDirectories != ledgerDirectories) {
                allLedgerDirs.addAll(Arrays.asList(indexDirectories));
            }

            try {
                Bookie.checkEnvironmentWithStorageExpansion(bkConf, driver, Arrays.asList(journalDirectories),
                                                            allLedgerDirs);
                return true;
            } catch (BookieException e) {
                LOG.error("Exception while updating cookie for storage expansion", e);
                return false;
            }
        });
    }

    private boolean listOrDeleteCookies(ServerConfiguration bkConf, boolean delete, boolean force) throws Exception {
        BookieSocketAddress bookieAddress = Bookie.getBookieAddress(bkConf);
        File[] journalDirs = bkConf.getJournalDirs();
        File[] ledgerDirs = bkConf.getLedgerDirs();
        File[] indexDirs = bkConf.getIndexDirs();
        File[] allDirs = ArrayUtils.addAll(journalDirs, ledgerDirs);
        if (indexDirs != null) {
            allDirs = ArrayUtils.addAll(allDirs, indexDirs);
        }

        File[] allCurDirs = Bookie.getCurrentDirectories(allDirs);
        List<File> allVersionFiles = new LinkedList<File>();
        File versionFile;
        for (File curDir : allCurDirs) {
            versionFile = new File(curDir, BookKeeperConstants.VERSION_FILENAME);
            if (versionFile.exists()) {
                allVersionFiles.add(versionFile);
            }
        }

        if (!allVersionFiles.isEmpty()) {
            if (delete) {
                boolean confirm = force;
                if (!confirm) {
                    confirm = IOUtils.confirmPrompt("Are you sure you want to delete Cookies locally?");
                }
                if (confirm) {
                    for (File verFile : allVersionFiles) {
                        if (!verFile.delete()) {
                            LOG.error("Failed to delete Local cookie file {}. So aborting deletecookie of Bookie: {}",
                                      verFile, bookieAddress);
                            return false;
                        }
                    }
                    LOG.info("Deleted Local Cookies of Bookie: {}", bookieAddress);
                } else {
                    LOG.info("Skipping deleting local Cookies of Bookie: {}", bookieAddress);
                }
            } else {
                LOG.info("Listing local Cookie Files of Bookie: {}", bookieAddress);
                for (File verFile : allVersionFiles) {
                    LOG.info(verFile.getCanonicalPath());
                }
            }
        } else {
            LOG.info("No local cookies for Bookie: {}", bookieAddress);
        }

        return runFunctionWithRegistrationManager(bkConf, rm -> {
            try {
                Versioned<Cookie> cookie = null;
                try {
                    cookie = Cookie.readFromRegistrationManager(rm, bookieAddress);
                } catch (BookieException.CookieNotFoundException nne) {
                    LOG.info("No cookie for {} in metadata store", bookieAddress);
                    return true;
                }

                if (delete) {
                    boolean confirm = force;
                    if (!confirm) {
                        confirm = IOUtils.confirmPrompt("Are you sure you want to delete Cookies from metadata store?");
                    }

                    if (confirm) {
                        cookie.getValue().deleteFromRegistrationManager(rm, bkConf, cookie.getVersion());
                        LOG.info("Deleted Cookie from metadata store for Bookie: {}", bookieAddress);
                    } else {
                        LOG.info("Skipping deleting cookie from metadata store for Bookie: {}", bookieAddress);
                    }
                }
            } catch (BookieException | IOException e) {
                return false;
            }
            return true;
        });
    }
}
