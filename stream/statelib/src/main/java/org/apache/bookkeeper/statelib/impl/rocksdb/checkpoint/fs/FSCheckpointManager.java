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
package org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint.fs;

import com.google.common.collect.Lists;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;
import org.apache.bookkeeper.statelib.api.checkpoint.CheckpointStore;

/**
 * Filesystem based checkpoint factory.
 */
public class FSCheckpointManager implements CheckpointStore {

    private final File rootPath;

    public FSCheckpointManager(File rootPath) {
        this.rootPath = rootPath;
    }

    private File getFullyQualifiedPath(String filePath) {
        return new File(rootPath, filePath);
    }

    @Override
    public List<String> listFiles(String filePath) throws IOException {
        String[] files = getFullyQualifiedPath(filePath).list();
        if (null == files) {
            return Collections.emptyList();
        }
        return Lists.newArrayList(files);
    }

    @Override
    public boolean fileExists(String filePath) throws IOException {
        return getFullyQualifiedPath(filePath).exists();
    }

    @Override
    public long getFileLength(String filePath) throws IOException {
        return getFullyQualifiedPath(filePath).length();
    }

    @Override
    public InputStream openInputStream(String filePath) throws IOException {
        return new FileInputStream(getFullyQualifiedPath(filePath));
    }

    @Override
    public OutputStream openOutputStream(String filePath) throws IOException {
        return new FileOutputStream(getFullyQualifiedPath(filePath));
    }

    @Override
    public void rename(String srcPath, String destPath) throws IOException {
        Files.move(
            Paths.get(getFullyQualifiedPath(srcPath).getAbsolutePath()),
            Paths.get(getFullyQualifiedPath(destPath).getAbsolutePath()),
            StandardCopyOption.ATOMIC_MOVE);
    }

    @Override
    public void deleteRecursively(String srcPath) throws IOException {
        MoreFiles.deleteRecursively(
            Paths.get(getFullyQualifiedPath(srcPath).getAbsolutePath()),
            RecursiveDeleteOption.ALLOW_INSECURE);
    }

    @Override
    public void delete(String srcPath) throws IOException {
        Files.delete(Paths.get(getFullyQualifiedPath(srcPath).getAbsolutePath()));
    }

    @Override
    public void createDirectories(String srcPath) throws IOException {
        Files.createDirectories(
            Paths.get(getFullyQualifiedPath(srcPath).getAbsolutePath()));
    }

    @Override
    public void close() {
        // no-op
    }
}
