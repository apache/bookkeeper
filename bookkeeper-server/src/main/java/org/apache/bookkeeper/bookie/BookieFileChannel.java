/**
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
package org.apache.bookkeeper.bookie;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * A FileChannel for the JournalChannel read and write, we can use this interface to extend the FileChannel
 * which we use in the JournalChannel.
 */
interface BookieFileChannel {

    /**
     * An interface for get the FileChannel from the provider.
     * @return
     */
    FileChannel getFileChannel() throws FileNotFoundException, IOException;

    /**
     * Check the given file if exists.
     *
     * @param file
     * @return
     */
    boolean fileExists(File file);

    /**
     * Get the file descriptor of the opened file.
     *
     * @return
     * @throws IOException
     */
    FileDescriptor getFD() throws IOException;

    /**
     * Close file channel and release all resources.
     */
    void close() throws IOException;
}
