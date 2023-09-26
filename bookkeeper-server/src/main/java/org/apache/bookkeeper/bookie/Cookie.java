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

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.google.protobuf.TextFormat;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.Set;
import org.apache.bookkeeper.bookie.BookieException.InvalidCookieException;
import org.apache.bookkeeper.bookie.BookieException.UnknownBookieIdException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.DataFormats.CookieFormat;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When a bookie starts for the first time it generates  a cookie, and stores
 * the cookie in registration manager as well as in the each of the local filesystem
 * directories it uses. This cookie is used to ensure that for the life of the
 * bookie, its configuration stays the same. If any of the bookie directories
 * becomes unavailable, the bookie becomes unavailable. If the bookie changes
 * port, it must also reset all of its data.
 * This is done to ensure data integrity. Without the cookie a bookie could
 * start with one of its ledger directories missing, so data would be missing,
 * but the bookie would be up, so the client would think that everything is ok
 * with the cluster. It's better to fail early and obviously.
 */
public class Cookie {
    private static final Logger LOG = LoggerFactory.getLogger(Cookie.class);

    static final int CURRENT_COOKIE_LAYOUT_VERSION = 5;
    private final int layoutVersion;
    private final String bookieId;
    private final String journalDirs;
    private final String ledgerDirs;
    private final String indexDirs;
    private final String instanceId;
    private static final String SEPARATOR = "\t";

    private Cookie(int layoutVersion, String bookieId, String journalDirs, String ledgerDirs, String instanceId,
                   String indexDirs) {
        this.layoutVersion = layoutVersion;
        this.bookieId = bookieId;
        this.journalDirs = journalDirs;
        this.ledgerDirs = ledgerDirs;
        this.instanceId = instanceId;
        this.indexDirs = indexDirs;
    }

    public static String encodeDirPaths(String[] dirs) {
        StringBuilder b = new StringBuilder();
        b.append(dirs.length);
        for (String d : dirs) {
            b.append(SEPARATOR).append(d);
        }
        return b.toString();
    }

    private static String[] decodeDirPathFromCookie(String s) {
        // the first part of the string contains a count of how many
        // directories are present; to skip it, we look for subString
        // from the first '/'
        return s.substring(s.indexOf(SEPARATOR) + SEPARATOR.length()).split(SEPARATOR);
    }

    String[] getLedgerDirPathsFromCookie() {
        return decodeDirPathFromCookie(ledgerDirs);
    }

    String[] getIndexDirPathsFromCookie() {
        if (null == indexDirs) {
            return null;
        }
        return decodeDirPathFromCookie(indexDirs);
    }

    /**
     * Receives 2 String arrays, that each contain a list of directory paths,
     * and checks if first is a super set of the second.
     *
     * @param superS
     * @param subS
     * @return true if superS is a superSet of subS; false otherwise
     */
    private boolean isSuperSet(String[] superS, String[] subS) {
        Set<String> superSet = Sets.newHashSet(superS);
        Set<String> subSet = Sets.newHashSet(subS);
        return superSet.containsAll(subSet);
    }

    private boolean verifyLedgerDirs(Cookie c, boolean checkIfSuperSet) {
        if (!checkIfSuperSet) {
            return ledgerDirs.equals(c.ledgerDirs);
        } else {
            return isSuperSet(decodeDirPathFromCookie(ledgerDirs), decodeDirPathFromCookie(c.ledgerDirs));
        }
    }

    private boolean verifyIndexDirs(Cookie c, boolean checkIfSuperSet) {
        // compatible logic:  version 5 introduce `indexDirs`, we shouldn't check `indexDirs` with the before version.
        if (this.layoutVersion >= 5 && c.layoutVersion < 5) {
            return true;
        }
        if (indexDirs == null && c.indexDirs == null) {
            return true;
        }
        if (indexDirs == null || c.indexDirs == null) {
            return false;
        }
        if (!checkIfSuperSet) {
            return indexDirs.equals(c.indexDirs);
        } else {
            return isSuperSet(decodeDirPathFromCookie(indexDirs), decodeDirPathFromCookie(c.indexDirs));
        }
    }

    private void verifyInternal(Cookie c, boolean checkIfSuperSet) throws BookieException.InvalidCookieException {
        String errMsg;
        if (c.layoutVersion < 3 && c.layoutVersion != layoutVersion) {
            errMsg = "Cookie is of too old version " + c.layoutVersion;
            LOG.error(errMsg);
            throw new BookieException.InvalidCookieException(errMsg);
        } else if (!(c.layoutVersion >= 3 && c.bookieId.equals(bookieId)
            && c.journalDirs.equals(journalDirs) && verifyLedgerDirs(c, checkIfSuperSet)
            && verifyIndexDirs(c, checkIfSuperSet))) {
            errMsg = "Cookie [" + this + "] is not matching with [" + c + "]";
            throw new BookieException.InvalidCookieException(errMsg);
        } else if ((instanceId == null && c.instanceId != null)
                || (instanceId != null && !instanceId.equals(c.instanceId))) {
            // instanceId should be same in both cookies
            errMsg = "instanceId " + instanceId
                    + " is not matching with " + c.instanceId;
            throw new BookieException.InvalidCookieException(errMsg);
        }
    }

    public void verify(Cookie c) throws BookieException.InvalidCookieException {
        verifyInternal(c, false);
    }

    public void verifyIsSuperSet(Cookie c) throws BookieException.InvalidCookieException {
        verifyInternal(c, true);
    }

    @Override
    public String toString() {
        if (layoutVersion <= 3) {
            return toStringVersion3();
        }
        CookieFormat.Builder builder = CookieFormat.newBuilder();
        builder.setBookieHost(bookieId);
        builder.setJournalDir(journalDirs);
        builder.setLedgerDirs(ledgerDirs);
        if (null != instanceId) {
            builder.setInstanceId(instanceId);
        }
        if (null != indexDirs) {
            builder.setIndexDirs(indexDirs);
        }

        StringBuilder b = new StringBuilder();
        b.append(layoutVersion).append("\n");
        b.append(builder.build());
        return b.toString();
    }

    private String toStringVersion3() {
        StringBuilder b = new StringBuilder();
        b.append(CURRENT_COOKIE_LAYOUT_VERSION).append("\n")
            .append(bookieId).append("\n")
            .append(journalDirs).append("\n")
            .append(ledgerDirs).append("\n");
        return b.toString();
    }

    private static Builder parse(BufferedReader reader) throws IOException {
        Builder cBuilder = Cookie.newBuilder();
        int layoutVersion = 0;
        String line = reader.readLine();
        if (null == line) {
            throw new EOFException("Exception in parsing cookie");
        }
        try {
            layoutVersion = Integer.parseInt(line.trim());
            cBuilder.setLayoutVersion(layoutVersion);
        } catch (NumberFormatException e) {
            throw new IOException("Invalid string '" + line.trim()
                    + "', cannot parse cookie.");
        }
        if (layoutVersion == 3) {
            cBuilder.setBookieId(reader.readLine());
            cBuilder.setJournalDirs(reader.readLine());
            cBuilder.setLedgerDirs(reader.readLine());
        } else if (layoutVersion >= 4) {
            CookieFormat.Builder cfBuilder = CookieFormat.newBuilder();
            TextFormat.merge(reader, cfBuilder);
            CookieFormat data = cfBuilder.build();
            cBuilder.setBookieId(data.getBookieHost());
            cBuilder.setJournalDirs(data.getJournalDir());
            cBuilder.setLedgerDirs(data.getLedgerDirs());
            // Since InstanceId is optional
            if (null != data.getInstanceId() && !data.getInstanceId().isEmpty()) {
                cBuilder.setInstanceId(data.getInstanceId());
            }
            if (null != data.getIndexDirs() && !data.getIndexDirs().isEmpty()) {
                cBuilder.setIndexDirs(data.getIndexDirs());
            }
        }
        return cBuilder;
    }

    public static Cookie parseFromBytes(byte[] bytes) throws IOException {
        try (BufferedReader reader = new BufferedReader(new StringReader(new String(bytes, UTF_8)))) {
            return parse(reader).build();
        }
    }

    public void writeToDirectory(File directory) throws IOException {
        File versionFile = new File(directory,
            BookKeeperConstants.VERSION_FILENAME);
        writeToFile(versionFile);
    }

    public void writeToFile (File versionFile) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(versionFile);
             BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos, UTF_8))) {
            bw.write(toString());
        }
    }

    /**
     * Writes cookie details to registration manager.
     *
     * @param rm registration manager
     * @param conf configuration
     * @param version version
     * @throws BookieException when fail to write the cookie.
     */
    public void writeToRegistrationManager(RegistrationManager rm, ServerConfiguration conf, Version version)
            throws BookieException {
        BookieId address = null;
        try {
            address = BookieImpl.getBookieId(conf);
        } catch (UnknownHostException e) {
            throw new UnknownBookieIdException(e);
        }
        byte[] data = toString().getBytes(UTF_8);
        rm.writeCookie(address, new Versioned<>(data, version));
    }

    /**
     * Deletes cookie from registration manager.
     *
     * @param rm registration manager
     * @param conf configuration
     * @param version cookie version
     * @throws BookieException when fail to delete cookie.
     */
    public void deleteFromRegistrationManager(RegistrationManager rm,
                                              ServerConfiguration conf,
                                              Version version) throws BookieException {
        BookieId address = null;
        try {
            address = BookieImpl.getBookieId(conf);
        } catch (UnknownHostException e) {
            throw new UnknownBookieIdException(e);
        }
        deleteFromRegistrationManager(rm, address, version);
    }

    /**
     * Delete cookie from registration manager.
     *
     * @param rm registration manager
     * @param address bookie address
     * @param version cookie version
     * @throws BookieException when fail to delete cookie.
     */
    public void deleteFromRegistrationManager(RegistrationManager rm,
                                              BookieId address,
                                              Version version) throws BookieException {
        if (!(version instanceof LongVersion)) {
            throw new IllegalArgumentException("Invalid version type, expected ZkVersion type");
        }

        rm.removeCookie(address, version);
    }

    /**
     * Generate cookie from the given configuration.
     *
     * @param conf configuration
     * @return cookie builder object
     * @throws UnknownHostException
     */
    public static Builder generateCookie(ServerConfiguration conf)
            throws UnknownHostException {
        Builder builder = Cookie.newBuilder();
        builder.setLayoutVersion(CURRENT_COOKIE_LAYOUT_VERSION);
        builder.setBookieId(BookieImpl.getBookieId(conf).toString());
        builder.setJournalDirs(Joiner.on(',').join(conf.getJournalDirNames()));
        builder.setLedgerDirs(encodeDirPaths(conf.getLedgerDirNames()));
        if (null != conf.getIndexDirNames()) {
            builder.setIndexDirs(encodeDirPaths(conf.getIndexDirNames()));
        }
        return builder;
    }

    /**
     * Read cookie from registration manager.
     *
     * @param rm registration manager
     * @param conf configuration
     * @return versioned cookie object
     * @throws BookieException when fail to read cookie
     */
    public static Versioned<Cookie> readFromRegistrationManager(RegistrationManager rm, ServerConfiguration conf)
            throws BookieException {
        try {
            return readFromRegistrationManager(rm, BookieImpl.getBookieId(conf));
        } catch (UnknownHostException e) {
            throw new UnknownBookieIdException(e);
        }
    }

    /**
     * Read cookie from registration manager for a given bookie <i>address</i>.
     *
     * @param rm registration manager
     * @param address bookie address
     * @return versioned cookie object
     * @throws BookieException when fail to read cookie
     */
    public static Versioned<Cookie> readFromRegistrationManager(RegistrationManager rm,
                                                         BookieId address) throws BookieException {
        Versioned<byte[]> cookieData = rm.readCookie(address);
        try {
            try (BufferedReader reader = new BufferedReader(
                    new StringReader(new String(cookieData.getValue(), UTF_8)))) {
                Builder builder = parse(reader);
                Cookie cookie = builder.build();
                return new Versioned<Cookie>(cookie, cookieData.getVersion());
            }
        } catch (IOException ioe) {
            throw new InvalidCookieException(ioe);
        }
    }

    /**
     * Returns cookie from the given directory.
     *
     * @param directory directory
     * @return cookie object
     * @throws IOException
     */
    public static Cookie readFromDirectory(File directory) throws IOException, FileNotFoundException {
        File versionFile = new File(directory, BookKeeperConstants.VERSION_FILENAME);
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(versionFile), UTF_8))) {
            return parse(reader).build();
        }
    }

    /**
     * Check whether the 'bookieHost' was created using a hostname or an IP
     * address. Represent as 'hostname/IPaddress' if the InetSocketAddress was
     * created using hostname. Represent as '/IPaddress' if the
     * InetSocketAddress was created using an IPaddress
     *
     * @return true if the 'bookieHost' was created using an IP address, false
     *         if the 'bookieHost' was created using a hostname
     */
    public boolean isBookieHostCreatedFromIp() throws IOException {
        String[] parts = bookieId.split(":");
        if (parts.length != 2) {
            // custom BookieId ?
            return false;
        }
        int port;
        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            // custom BookieId ?
            return false;
        }

        InetSocketAddress addr = new InetSocketAddress(parts[0], port);
        return addr.toString().startsWith("/");
    }

    /**
     * Cookie builder.
     */
    public static class Builder {
        private int layoutVersion = CURRENT_COOKIE_LAYOUT_VERSION;
        private String bookieId = null;
        private String journalDirs = null;
        private String ledgerDirs = null;
        private String instanceId = null;
        private String indexDirs = null;

        private Builder() {
        }

        private Builder(int layoutVersion, String bookieId, String journalDirs, String ledgerDirs,
                        String instanceId, String indexDirs) {
            this.layoutVersion = layoutVersion;
            this.bookieId = bookieId;
            this.journalDirs = journalDirs;
            this.ledgerDirs = ledgerDirs;
            this.instanceId = instanceId;
            this.indexDirs = indexDirs;
        }

        public Builder setLayoutVersion(int layoutVersion) {
            this.layoutVersion = layoutVersion;
            return this;
        }

        public Builder setBookieId(String bookieId) {
            this.bookieId = bookieId;
            return this;
        }

        public Builder setJournalDirs(String journalDirs) {
            this.journalDirs = journalDirs;
            return this;
        }

        public Builder setLedgerDirs(String ledgerDirs) {
            this.ledgerDirs = ledgerDirs;
            return this;
        }

        public Builder setInstanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder setIndexDirs(String indexDirs) {
            this.indexDirs = indexDirs;
            return this;
        }

        public Cookie build() {
            return new Cookie(layoutVersion, bookieId, journalDirs, ledgerDirs, instanceId, indexDirs);
        }
    }

    /**
     * Returns Cookie builder.
     *
     * @return cookie builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Returns Cookie builder with the copy of given oldCookie.
     *
     * @param oldCookie build new cookie from this cookie
     * @return cookie builder
     */
    public static Builder newBuilder(Cookie oldCookie) {
        return new Builder(oldCookie.layoutVersion, oldCookie.bookieId, oldCookie.journalDirs, oldCookie.ledgerDirs,
                oldCookie.instanceId, oldCookie.indexDirs);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Cookie) {
            Cookie otherCookie = (Cookie) other;
            return layoutVersion == otherCookie.layoutVersion
                && Objects.equals(bookieId, otherCookie.bookieId)
                && Objects.equals(journalDirs, otherCookie.journalDirs)
                && Objects.equals(ledgerDirs, otherCookie.ledgerDirs)
                && Objects.equals(instanceId, otherCookie.instanceId)
                && Objects.equals(indexDirs, otherCookie.indexDirs);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(bookieId, journalDirs, ledgerDirs, instanceId, indexDirs);
    }
}
