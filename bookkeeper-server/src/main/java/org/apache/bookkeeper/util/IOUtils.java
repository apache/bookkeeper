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
package org.apache.bookkeeper.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.slf4j.Logger;

/**
 * An utility class for I/O related functionality.
 */
public class IOUtils {

    /**
     * Close the Closeable objects and <b>ignore</b> any {@link IOException} or
     * null pointers. Must only be used for cleanup in exception handlers.
     *
     * @param log
     *            the log to record problems to at debug level. Can be null.
     * @param closeables
     *            the objects to close
     */
    public static void close(Logger log, java.io.Closeable... closeables) {
        for (java.io.Closeable c : closeables) {
            close(log, c);
        }
    }

    /**
     * Close the Closeable object and <b>ignore</b> any {@link IOException} or
     * null pointers. Must only be used for cleanup in exception handlers.
     *
     * @param log
     *            the log to record problems to at debug level. Can be null.
     * @param closeable
     *            the objects to close
     */
    public static void close(Logger log, java.io.Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                if (log != null && log.isDebugEnabled()) {
                    log.debug("Exception in closing " + closeable, e);
                }
            }
        }
    }

    /**
     * Confirm prompt for the console operations.
     *
     * @param prompt
     *            Prompt message to be displayed on console
     * @return Returns true if confirmed as 'Y', returns false if confirmed as
     *         'N'
     * @throws IOException
     */
    public static boolean confirmPrompt(String prompt) throws IOException {
        while (true) {
            System.out.print(prompt + " (Y or N) ");
            StringBuilder responseBuilder = new StringBuilder();
            while (true) {
                int c = System.in.read();
                if (c == -1 || c == '\r' || c == '\n') {
                    break;
                }
                responseBuilder.append((char) c);
            }

            String response = responseBuilder.toString();
            if (response.equalsIgnoreCase("y")
                    || response.equalsIgnoreCase("yes")) {
                return true;
            } else if (response.equalsIgnoreCase("n")
                    || response.equalsIgnoreCase("no")) {
                return false;
            }
            System.out.println("Invalid input: " + response);
            // else ask them again
        }
    }

    /**
     * Write a ByteBuffer to a WritableByteChannel, handling short writes.
     *
     * @param bc               The WritableByteChannel to write to
     * @param buf              The input buffer
     * @throws IOException     On I/O error
     */
    public static void writeFully(WritableByteChannel bc, ByteBuffer buf)
            throws IOException {
        do {
            bc.write(buf);
        } while (buf.remaining() > 0);
    }


    /**
     * Create a temp directory with given <i>prefix</i> and <i>suffix</i>.
     *
     * @param prefix
     *          prefix of the directory name
     * @param suffix
     *          suffix of the directory name
     * @return directory created
     * @throws IOException
     */
    public static File createTempDir(String prefix, String suffix)
            throws IOException {
        return createTempDir(prefix, suffix, null);
    }

    /**
     * Create a temp directory with given <i>prefix</i> and <i>suffix</i> in the specified <i>dir</i>.
     *
     * @param prefix
     *          prefix of the directory name
     * @param suffix
     *          suffix of the directory name
     * @param dir
     *          The directory in which the file is to be created,
     *          or null if the default temporary-file directory is to be used
     * @return directory created
     * @throws IOException
     */
    public static File createTempDir(String prefix, String suffix, File dir)
            throws IOException {
        File tmpDir = File.createTempFile(prefix, suffix, dir);
        if (!tmpDir.delete()) {
            throw new IOException("Couldn't delete directory " + tmpDir);
        }
        if (!tmpDir.mkdir()) {
            throw new IOException("Couldn't create directory " + tmpDir);
        }
        return tmpDir;
    }

    /**
     * Create a temp directory with given <i>prefix</i> and <i>suffix</i>.
     *
     * @param prefix
     *          prefix of the directory name
     * @param suffix
     *          suffix of the directory name
     * @return directory created
     * @throws IOException
     */
    public static File createTempFileAndDeleteOnExit(String prefix, String suffix)
            throws IOException {
        File tmpDir = File.createTempFile(prefix, suffix);
        tmpDir.deleteOnExit();
        return tmpDir;
    }
}
