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
import java.util.HashSet;
import java.util.Set;

import org.apache.bookkeeper.bookie.Bookie;

public class TestUtils {
    public static boolean hasLogFiles(File ledgerDirectory, boolean partial, Integer... logsId) {
        boolean result = partial ? false : true;
        Set<Integer> logs = new HashSet<Integer>();
        for (File file : Bookie.getCurrentDirectory(ledgerDirectory).listFiles()) {
            if (file.isFile()) {
                String name = file.getName();
                if (!name.endsWith(".log")) {
                    continue;
                }
                logs.add(Integer.parseInt(name.split("\\.")[0], 16));
            }
        }
        for (Integer logId : logsId) {
            boolean exist = logs.contains(logId);
            if ((partial && exist)
                    || (!partial && !exist)) {
                return !result;
            }
        }
        return result;
    }
}
