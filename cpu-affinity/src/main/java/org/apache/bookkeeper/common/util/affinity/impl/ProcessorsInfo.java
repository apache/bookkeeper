/**
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
package org.apache.bookkeeper.common.util.affinity.impl;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;

/**
 * Discover the list of processors from /proc/cpuinfo.
 */
class ProcessorsInfo {

    private static final Charset ENCODING = StandardCharsets.US_ASCII;

    /**
     * Given one cpu id, return all CPUs that are sharing the same core.
     */
    public Set<Integer> getCpusOnSameCore(int targetCpu) {
        Set<Integer> result = new TreeSet<>();
        int targetCore = cpus.get(targetCpu);

        cpus.forEach((cpu, core) -> {
            if (core == targetCore) {
                result.add(cpu);
            }
        });

        return result;
    }

    private final Map<Integer, Integer> cpus = new TreeMap<>();

    static ProcessorsInfo parseCpuInfo(String cpuInfoString) {
        ProcessorsInfo pi = new ProcessorsInfo();

        for (String cpu : cpuInfoString.split("\n\n")) {
            int cpuId = -1;
            int coreId = -1;

            for (String line : cpu.split("\n")) {
                String[] parts = line.split(":", 2);
                String key = StringUtils.trim(parts[0]);
                String value = StringUtils.trim(parts[1]);

                if (key.equals("core id")) {
                    coreId = Integer.parseInt(value);
                } else if (key.equals("processor")) {
                    cpuId = Integer.parseInt(value);
                } else {
                    // ignore
                }
            }

            checkArgument(cpuId >= 0);
            checkArgument(coreId >= 0);
            pi.cpus.put(cpuId, coreId);
        }

        return pi;
    }

    static ProcessorsInfo parseCpuInfo() {
        try {
            return parseCpuInfo(new String(Files.readAllBytes(Paths.get("/proc/cpuinfo")), ENCODING));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
