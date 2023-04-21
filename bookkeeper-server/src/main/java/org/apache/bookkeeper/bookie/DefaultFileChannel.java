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

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import org.apache.bookkeeper.conf.ServerConfiguration;

/**
 * Default FileChannel for bookie to read and write.
 *
 */
public class DefaultFileChannel implements BookieFileChannel {
    private final File file;
    private RandomAccessFile randomAccessFile;
    private final ServerConfiguration configuration;

    DefaultFileChannel(File file, ServerConfiguration serverConfiguration) throws IOException {
        this.file = file;
        this.configuration = serverConfiguration;
    }

    @Override
    public FileChannel getFileChannel() throws FileNotFoundException {
        synchronized (this) {
            if (randomAccessFile == null) {
                randomAccessFile = new RandomAccessFile(file, "rw");
            }
            return randomAccessFile.getChannel();
        }
    }

    @Override
    public boolean fileExists(File file) {
        return file.exists();
    }

    @Override
    public FileDescriptor getFD() throws IOException {
        synchronized (this) {
            if (randomAccessFile == null) {
                throw new IOException("randomAccessFile is null, please initialize it by calling getFileChannel");
            }
            return randomAccessFile.getFD();
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (randomAccessFile != null) {
                randomAccessFile.close();
            }
        }
    }
}
