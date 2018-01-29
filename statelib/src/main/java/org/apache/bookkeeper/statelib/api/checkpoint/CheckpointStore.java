/*
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
package org.apache.bookkeeper.statelib.api.checkpoint;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * A checkpoint factory that provides inputstream and outputstream for reading checkpoints.
 */
public interface CheckpointStore extends AutoCloseable {

    List<String> listFiles(String filePath) throws IOException;

    boolean fileExists(String filePath) throws IOException;

    long getFileLength(String filePath) throws IOException;

    InputStream openInputStream(String filePath) throws IOException;

    OutputStream openOutputStream(String filePath) throws IOException;

    void rename(String srcPath, String destPath) throws IOException;

    void deleteRecursively(String srcPath) throws IOException;

    void delete(String srcPath) throws IOException;

    void createDirectories(String srcPath) throws IOException;

    /**
     * Close the checkpoint store.
     */
    @Override
    void close();
}
