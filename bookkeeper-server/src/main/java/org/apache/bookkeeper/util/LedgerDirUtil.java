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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LedgerDirUtil {

    public static final Pattern FILE_PATTERN = Pattern.compile("^([0-9a-fA-F]+)\\.log$");
    public static final Pattern COMPACTED_FILE_PATTERN =
            Pattern.compile("^([0-9a-fA-F]+)\\.log\\.([0-9a-fA-F]+)\\.compacted$");

    public static List<Integer> logIdsInDirectory(File directory) {
        List<Integer> ids = new ArrayList<>();
        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null && files.length > 0) {
                for (File f : files) {
                    Matcher m = FILE_PATTERN.matcher(f.getName());
                    if (m.matches()) {
                        int logId = Integer.parseUnsignedInt(m.group(1), 16);
                        ids.add(logId);
                    }
                }
            }
        }
        return ids;
    }

    public static List<Integer> compactedLogIdsInDirectory(File directory) {
        List<Integer> ids = new ArrayList<>();
        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null && files.length > 0) {
                for (File f : files) {
                    Matcher m = COMPACTED_FILE_PATTERN.matcher(f.getName());
                    if (m.matches()) {
                        int logId = Integer.parseUnsignedInt(m.group(1), 16);
                        ids.add(logId);
                    }
                }
            }
        }
        return ids;
    }
}