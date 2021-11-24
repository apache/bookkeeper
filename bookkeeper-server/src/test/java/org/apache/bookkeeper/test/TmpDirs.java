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
package org.apache.bookkeeper.test;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;

/**
 * Utility class for managing tmp directories in tests.
 */
public class TmpDirs {
    private final List<File> tmpDirs = new LinkedList<>(); // retained to delete files

    public File createNew(String prefix, String suffix) throws Exception {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tmpDirs.add(dir);
        return dir;
    }

    public void cleanup() throws Exception {
        for (File f : tmpDirs) {
            FileUtils.deleteDirectory(f);
        }
    }

    public List<File> getDirs() {
        return tmpDirs;
    }
}