/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * A {@code BufferedChannelBase} adds functionality to an existing file channel, the ability
 * to buffer the input and output data. This class is a base class for wrapping the {@link FileChannel}.
 */
public abstract class BufferedChannelBase {
    static class BufferedChannelClosedException extends IOException {
        BufferedChannelClosedException() {
            super("Attempting to access a file channel that has already been closed");
        }
    }

    protected final FileChannel fileChannel;

    protected BufferedChannelBase(FileChannel fc) {
        this.fileChannel = fc;
    }

    protected FileChannel validateAndGetFileChannel() throws IOException {
        // Even if we have BufferedChannelBase objects in the cache, higher layers should
        // guarantee that once a log file has been closed and possibly deleted during garbage
        // collection, attempts will not be made to read from it
        if (!fileChannel.isOpen()) {
            throw new BufferedChannelClosedException();
        }
        return fileChannel;
    }

    /**
     * Get the current size of the underlying FileChannel.
     * @return
     */
    public long size() throws IOException {
        return validateAndGetFileChannel().size();
    }

}
