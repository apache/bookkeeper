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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.SortedSet;
import java.util.TreeSet;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;

@UtilityClass
@Slf4j
class IsolatedProcessors {

    private static final Charset ENCODING = StandardCharsets.US_ASCII;

    private static final String ISOLATED_CPU_PATH = "/sys/devices/system/cpu/isolated";

    @SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
    static SortedSet<Integer> get() throws IOException {
        return parseProcessorRange(new String(Files.readAllBytes(Paths.get(ISOLATED_CPU_PATH)), ENCODING));
    }

    static SortedSet<Integer> parseProcessorRange(String range) {
        SortedSet<Integer> processors = new TreeSet<>();

        for (String part : StringUtils.trim(range).split(",")) {
            if (part.contains("-")) {
                // This is a range, eg: 1-5 with both edges included
                String[] parts = part.split("-");
                int first = Integer.parseInt(parts[0]);
                int last = Integer.parseInt(parts[1]);

                for (int i = first; i <= last; i++) {
                    processors.add(i);
                }
            } else if (!part.isEmpty()) {
                processors.add(Integer.parseInt(part));
            }
        }

        return processors;
    }

    /**
     * Instruct Linux to disable a particular CPU. This is used to disable hyper-threading on a particular core, by
     * shutting down the cpu that shares the same core.
     */
    static void disableCpu(int cpu) throws IOException {
        changeCpuStatus(cpu, false);
    }

    static void enableCpu(int cpu) throws IOException {
        changeCpuStatus(cpu, true);
    }

    /**
     * Instruct Linux to disable a particular CPU. This is used to disable hyper-threading on a particular core, by
     * shutting down the cpu that shares the same core.
     */
    private static void changeCpuStatus(int cpu, boolean enable) throws IOException {
        Path cpuPath = Paths.get(String.format("/sys/devices/system/cpu/cpu%d/online", cpu));

        boolean currentState = Integer
                .parseInt(StringUtils.trim(new String(Files.readAllBytes(cpuPath), ENCODING))) != 0;

        if (currentState != enable) {
            Files.write(cpuPath, (enable ? "1\n" : "0\n").getBytes(ENCODING), StandardOpenOption.TRUNCATE_EXISTING);
            log.info("{} CPU {}", enable ? "Enabled" : "Disabled", cpu);
        }
    }
}
