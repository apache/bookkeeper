/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bookkeeper.bookie;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Charsets.UTF_8;

import static org.apache.bookkeeper.util.BookKeeperConstants.BOOKIE_STATUS_FILENAME;

public class BookieStatus {
    static Logger LOG = LoggerFactory.getLogger(BookieStatus.class);
    static final int CURRENT_STATUS_LAYOUT_VERSION = 1;

    enum BookieMode {
        READ_ONLY,
        READ_WRITE;
    }

    private final static long INVALID_UPDATE_TIME = -1;

    private int layoutVersion;
    private long lastUpdateTime;
    private BookieMode bookieMode;


    BookieStatus() {
        this.bookieMode = BookieMode.READ_WRITE;
        this.layoutVersion = CURRENT_STATUS_LAYOUT_VERSION;
        this.lastUpdateTime = INVALID_UPDATE_TIME;
    }

    public boolean isInWritable() {
        return bookieMode.equals(BookieMode.READ_WRITE);
    }

    synchronized boolean setToWritableMode() {
        if (!bookieMode.equals(BookieMode.READ_WRITE)) {
            bookieMode = BookieMode.READ_WRITE;
            this.lastUpdateTime = System.currentTimeMillis();
            return true;
        }
        return false;
    }

    boolean isInReadOnlyMode() {
        return bookieMode.equals(BookieMode.READ_ONLY);
    }

    synchronized boolean setToReadOnlyMode() {
        if (!bookieMode.equals(BookieMode.READ_ONLY)) {
            bookieMode = BookieMode.READ_ONLY;
            this.lastUpdateTime = System.currentTimeMillis();
            return true;
        }
        return false;
    }

    /**
     * Write bookie status to multiple directories in best effort
     *
     * @param directories list of directories to write to
     * @return true if any write succeed, false if all failed
     *
     */
    synchronized boolean writeToDirectories(List<File> directories) {
        boolean success = false;
        for (File dir : directories) {
            try {
                File statusFile = new File(dir, BOOKIE_STATUS_FILENAME);
                writeToFile(statusFile, toString());
                success = true;
            } catch (IOException e) {
                LOG.warn("IOException while trying to write bookie status to directory {}." +
                    " This is fine if not all directories are failed.", dir);
            }
        }
        if(success){
            LOG.info("Successfully persist bookie status {}", this.bookieMode);
        } else {
            LOG.warn("Failed to persist bookie status {}", this.bookieMode);
        }
        return success;
    }

    /**
     * Write content to the file. If file does not exist, it will create one.
     *
     * @param file file that you want to write to
     * @param body content to write
     * @throws IOException
     */
    private void writeToFile(File file, String body) throws IOException {
        FileOutputStream fos = new FileOutputStream(file);
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(fos, UTF_8));
            bw.write(body);
        } finally {
            if (bw != null) {
                bw.close();
            }
            fos.close();
        }
    }

    /**
     * Read bookie status from the status files, and update the bookie status if read succeed.
     * If a status file is not readable or not found, it will skip and try to read from the next file.
     *
     * @param directories list of directories that store the status file
     * @return true if successfully read from file, otherwise false.
     */
    synchronized boolean readFromDirectories(List<File> directories) {
        boolean success = false;
        for (File dir : directories) {
            File statusFile = new File(dir, BOOKIE_STATUS_FILENAME);
            try {
                BookieStatus status = readFromFile(statusFile);
                if (status != null && status.lastUpdateTime > this.lastUpdateTime) {
                    this.lastUpdateTime = status.lastUpdateTime;
                    this.layoutVersion = status.layoutVersion;
                    this.bookieMode = status.bookieMode;
                    success = true;
                }
            } catch (IOException e) {
                LOG.warn("IOException while trying to read bookie status from directory {}." +
                    " This is fine if not all directories failed.", dir);
            } catch (IllegalArgumentException e ){
                LOG.warn("IllegalArgumentException while trying to read bookie status from directory {}." +
                    " This is fine if not all directories failed.", dir);
            }
        }
        if (success) {
            LOG.info("Successfully retrieve bookie status {} from disks.", this.bookieMode);
        } else {
            LOG.warn("Failed to retrieve bookie status from disks." +
                    " Fall back to current or default bookie status: {}", this.bookieMode);
        }
        return success;
    }


    /**
     * Function to read the bookie status from a single file
     *
     * @param file file to read from
     * @return BookieStatus if not error, null if file not exist or any exception happens
     * @throws IOException
     */
    private BookieStatus readFromFile(File file) throws IOException, IllegalArgumentException {
        if (!file.exists()) {
            return null;
        }
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(new FileInputStream(file), UTF_8));
        try {
            return parse(reader);
        } finally {
            reader.close();
        }
    }

    /**
     * Parse the bookie status object using appropriate layout version
     *
     * @param reader
     * @return BookieStatus if parse succeed, otherwise return null
     * @throws IOException
     */
    public BookieStatus parse(BufferedReader reader) throws IOException, IllegalArgumentException {
        BookieStatus status = new BookieStatus();
        String line = reader.readLine();
        if (line == null || line.trim().isEmpty()) {
            LOG.debug("Empty line when parsing bookie status");
            return null;
        }
        String[] parts = line.split(",");
        if (parts.length == 0) {
            LOG.debug("Error in parsing bookie status: {}", line);
            return null;
        }
        status.layoutVersion = Integer.parseInt(parts[0].trim());
        if (status.layoutVersion == 1 && parts.length == 3) {
            status.bookieMode = BookieMode.valueOf(parts[1]);
            status.lastUpdateTime = Long.parseLong(parts[2].trim());
            return status;
        }
        return null;

    }


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(CURRENT_STATUS_LAYOUT_VERSION);
        builder.append(",");
        builder.append(bookieMode);
        builder.append(",");
        builder.append(System.currentTimeMillis());
        builder.append("\n");
        return builder.toString();
    }

}
