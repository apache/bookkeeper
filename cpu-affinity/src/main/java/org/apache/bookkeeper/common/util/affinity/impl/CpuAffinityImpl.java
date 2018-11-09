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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of CPU Affinity functionality.
 */
@UtilityClass
@Slf4j
public class CpuAffinityImpl {

    private static boolean isInitialized = false;
    private static boolean isSupported;

    // Id of CPU cores acquired by this process
    private static final SortedSet<Integer> acquiredProcessors = new TreeSet<>();

    // Ids of processors that were isolated by Linux at boot time. This is the set
    // of processors that can acquired by this or other processes.
    private static SortedSet<Integer> isolatedProcessors = null;

    private static ProcessorsInfo processorsInfo = null;

    public static synchronized void acquireCore() {
        if (!isInitialized) {
            init();
        }

        if (!isSupported) {
            throw new RuntimeException("CPU Affinity not supported in current environment");
        }

        if (!CpuAffinityJni.isRoot()) {
            throw new RuntimeException("CPU Affinity can only be set if the process is running as root");
        }

        try {
            int cpu = pickAvailableCpu();
            CpuAffinityJni.setAffinity(cpu);

            log.info("Thread {} has successfully acquired ownership of cpu {}", Thread.currentThread().getName(), cpu);
        } catch (IOException e) {
            throw new RuntimeException("Failed to acquire CPU core: " + e.getMessage());
        }
    }

    private static final String LOCK_FILE_PREFIX = Paths.get(System.getProperty("java.io.tmpdir"), "cpu-lock-")
            .toString();

    /**
     * Other than the cores acquired by this process, there might be other processes on the same host trying to acquire
     * the available cores.
     *
     * <p>We use file-locks to ensure that other processes are aware of which CPUs are taken and that these locks are
     * automatically released if the process crashes.
     */
    private static synchronized int pickAvailableCpu() throws IOException {
        if (isolatedProcessors == null) {
            isolatedProcessors = IsolatedProcessors.get();
        }
        for (int isolatedCpu : isolatedProcessors) {
            if (log.isDebugEnabled()) {
                log.debug("Checking CPU {}", isolatedCpu);
            }
            if (acquiredProcessors.contains(isolatedCpu)) {
                if (log.isDebugEnabled()) {
                    log.debug("Ignoring CPU {} since it's already acquired", isolatedCpu);
                }
                continue;
            }

            if (tryAcquireCpu(isolatedCpu)) {
                if (log.isDebugEnabled()) {
                    log.debug("Using CPU {}", isolatedCpu);
                }
                return isolatedCpu;
            }
        }

        throw new RuntimeException(
                "There is no available isolated CPU to acquire for thread " + Thread.currentThread().getName());
    }

    private static boolean tryAcquireCpu(int targetCpu) throws IOException {
        // First, acquire lock on all the cpus that share the same core as target cpu
        if (processorsInfo == null) {
            processorsInfo = ProcessorsInfo.parseCpuInfo();
        }

        Set<Integer> cpusToAcquire = processorsInfo.getCpusOnSameCore(targetCpu);
        List<Closeable> acquiredCpus = new ArrayList<>();

        for (int cpu : cpusToAcquire) {
            Closeable lock = tryAcquireFileLock(cpu);
            if (lock == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Failed to acquire lock on CPU {}", cpu);
                }

                // Failed to acquire one cpu, release the rest that were already locked
                for (Closeable l : acquiredCpus) {
                    l.close();
                }

                return false;
            } else {
                acquiredCpus.add(lock);
            }
        }

        // At this point, we have ownership of all required cpus
        // Make sure the requested CPU is enabled and that all other CPUs on the same core are disabled, so that
        // hyper-threading will not affect latency.
        for (int cpu : cpusToAcquire) {
            if (cpu == targetCpu) {
                IsolatedProcessors.enableCpu(cpu);
            } else {
                IsolatedProcessors.disableCpu(cpu);
            }

            acquiredProcessors.add(cpu);
        }
        return true;
    }

    /**
     * Try to acquire a lock on a particular cpu.
     *
     * @return null if the lock was not available
     * @return a {@link Closeable} lock object if the lock was acquired
     */
    private static Closeable tryAcquireFileLock(int cpu) throws IOException {
        String lockPath = LOCK_FILE_PREFIX + cpu;

        RandomAccessFile file = null;
        FileChannel channel = null;
        FileLock lock = null;

        try {
            file = new RandomAccessFile(new File(lockPath), "rw");
            channel = file.getChannel();
            lock = channel.tryLock();

            if (lock == null) {
                return null;
            } else {
                final FileLock finalLock = lock;
                final FileChannel finalChannel = channel;
                final RandomAccessFile finalFile = file;
                return () -> {
                    // Closable object
                    finalLock.close();
                    finalChannel.close();
                    finalFile.close();
                };
            }
        } finally {
            if (lock == null) {
                if (channel != null) {
                    channel.close();
                }

                if (file != null) {
                    file.close();
                }
            }
        }
    }

    private static void init() {
        try {
            // Since this feature is only available in Linux, there's no point
            // in checking for MacOS jnilib or Windows dll extensions
            NativeUtils.loadLibraryFromJar("/lib/libcpu-affinity.so");
            isSupported = CpuAffinityJni.isAvailable();
        } catch (final Exception | UnsatisfiedLinkError e) {
            log.warn("Unable to load CPU affinity library: {}", e.getMessage(), e);
            isSupported = false;
        } finally {
            isInitialized = true;
        }
    }

}
