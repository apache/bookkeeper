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

package org.apache.bookkeeper.bookie;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import org.apache.bookkeeper.conf.ServerConfiguration;

/**
 * An interface of the FileChannelProvider.
 */
public interface FileChannelProvider extends Closeable {
    /**
     *
     * @param providerClassName Provided class name for file channel.
     * @return FileChannelProvider. A file channel provider loaded from providerClassName
     * @throws IOException Possible IOException.
     */
    static FileChannelProvider newProvider(String providerClassName) throws IOException {
        try {
            Class<?> providerClass = Class.forName(providerClassName);
            Object obj = providerClass.getConstructor().newInstance();
            return (FileChannelProvider) obj;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Get the BookieFileChannel with the given file and configuration.
     *
     * @param file File path related to bookie.
     * @param configuration Server configuration.
     * @return BookieFileChannel related to file parameter.
     * @throws IOException Possible IOException.
     */
    BookieFileChannel open(File file, ServerConfiguration configuration) throws IOException;

    /**
     * Close bookieFileChannel.
     * @param bookieFileChannel The bookieFileChannel to be closed.
     * @throws IOException Possible IOException.
     */
    void close(BookieFileChannel bookieFileChannel) throws IOException;

    /**
     * Whether support reuse file. Default is false.
     *
     * @return
     */
    default boolean supportReuseFile() {
        return false;
    }

    /**
     * Notify the rename source file name to the target file name operation.
     * @param source
     * @param target
     */
    default void notifyRename(File source, File target) {

    }
}
