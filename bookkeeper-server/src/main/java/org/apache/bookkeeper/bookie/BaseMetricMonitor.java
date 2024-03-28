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

package org.apache.bookkeeper.bookie;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.bookkeeper.bookie.stats.BaseMetricMonitorStats;
import org.apache.bookkeeper.bookie.stats.BookieStats;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Thread to monitor the journal disk periodically.
 */
public class BaseMetricMonitor {
    public static final Logger LOG = LoggerFactory.getLogger(BaseMetricMonitor.class);

    private final static int INVALID = -1;
    private final long intervalMs;
    private final int metricSlideWindowSize;
    private int curSliceIdx = 0;
    private final Map<DiskLocation, List<DiskIOStat>> journalIoStats = new HashMap<>();
    private final Map<DiskLocation, List<DiskIOStat>> ledgerIoStats = new HashMap<>();
    private final Map<String, CPUStat> cpuStats = new HashMap<>();
    private final WriteByteStat writeByteStat;
    private final BaseMetricMonitorStats baseMetricMonitorStats;
    private ScheduledExecutorService executor;
    private ScheduledFuture<?> checkTask;
    private final BookieStats bookieStats;
    private int updateBaseMetricAfterUpdateStatCount;
    private boolean reportStatByDelta;
    private final BaseMetric baseMetric = new BaseMetric();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    private class DiskLocation {

        private final String diskName;
        private final File location;
        DiskLocation(File location) throws IOException {
            this.location = location;
            FileStore fs = Files.getFileStore(location.toPath());
            this.diskName = Paths.get(fs.name()).getFileName().toString();
        }

        public String getDiskName() {
            return diskName;
        }

        @Override
        public String toString() {
            return location.toString() + " disk: " + diskName;
        }
        @Override
        public int hashCode() {
            return location.hashCode();
        }
    }

    private class DiskIOStat {
        private long lastTotalTicks;
        private long lastStatTime;
        private int util;

        public int getUtil() {
            return util;
        }

        public void setUtil(int util) {
            if (util == INVALID) {
                this.util = util;
                return;
            }
            if (util <= 100 && util >= 0) {
                this.util = util;
            } else if (util < 0) {
                this.util = 0;
            } else {
                this.util = 100;
            }
        }

        public long getLastTotalTicks() {
            return lastTotalTicks;
        }

        public void setLastTotalTicks(long lastTotalTicks) {
            this.lastTotalTicks = lastTotalTicks;
        }

        public long getLastStatTime() {
            return lastStatTime;
        }

        public void setLastStatTime(long lastStatTime) {
            this.lastStatTime = lastStatTime;
        }
    }

    private class CPUStat {
        private long lastFreeTime;
        private long lastTotalTime;
        private int cpuUsedRate;

        public long getLastFreeTime() {
            return lastFreeTime;
        }

        public void setLastFreeTime(long lastFreeTime) {
            this.lastFreeTime = lastFreeTime;
        }

        public long getLastTotalTime() {
            return lastTotalTime;
        }

        public void setLastTotalTime(long lastTotalTime) {
            this.lastTotalTime = lastTotalTime;
        }

        public int getCpuUsedRate() {
            return cpuUsedRate;
        }

        public void setCpuUsedRate(int cpuUsedRate) {
            if (cpuUsedRate == INVALID) {
                this.cpuUsedRate = INVALID;
                return;
            }
            if (cpuUsedRate <= 100 && cpuUsedRate >= 0) {
                this.cpuUsedRate = cpuUsedRate;
            } else if (cpuUsedRate < 0) {
                this.cpuUsedRate = 0;
            } else {
                this.cpuUsedRate = 100;
            }
        }
    }

    private class WriteByteStat {
        private long lastWriteByte;
        private long writeBytePerSecond;
        private long lastStatTime;

        public long getLastWriteByte() {
            return lastWriteByte;
        }

        public void setLastWriteByte(long lastWriteByte) {
            this.lastWriteByte = lastWriteByte;
        }

        public long getWriteBytePerSecond() {
            return writeBytePerSecond;
        }

        public void setWriteBytePerSecond(long writeBytePerSecond) {
            this.writeBytePerSecond = writeBytePerSecond;
        }

        public long getLastStatTime() {
            return lastStatTime;
        }

        public void setLastStatTime(long lastStatTime) {
            this.lastStatTime = lastStatTime;
        }
    }

    BaseMetricMonitor(ServerConfiguration conf, List<File> journalDirectories,
        List<File> ledgerDirectories, StatsLogger statsLogger, BookieStats bookieStats) throws IOException {
        if (!SystemUtils.IS_OS_LINUX) {
            String msg = "BaseMetricMonitor does not start, only Linux OS release support!";
            LOG.info(msg);
            throw new IOException(msg);
        }
        this.intervalMs = conf.getBaseMetricMonitorStatIntervalMs();
        this.metricSlideWindowSize = conf.getBaseMetricMonitorMetricSlideWindowSize();
        this.updateBaseMetricAfterUpdateStatCount = metricSlideWindowSize;
        this.bookieStats = bookieStats;

        initIoStat(journalIoStats, journalDirectories);
        LOG.info("BaseMetricMonitor adds ioStat for journal disk {}",
            journalIoStats.keySet().stream().map(DiskLocation::getDiskName).collect(Collectors.toList()));

        initIoStat(ledgerIoStats, ledgerDirectories);
        LOG.info("BaseMetricMonitor adds ioStat for ledger disk {}",
            ledgerIoStats.keySet().stream().map(DiskLocation::getDiskName).collect(Collectors.toList()));

        try {
            getUsedCPU(cpuStats);
            LOG.info("BaseMetricMonitor adds cpuStat for cpu {}", new ArrayList<>(cpuStats.keySet()));
        } catch (IOException e) {
            LOG.warn("BaseMetricMonitor getUsedCPU failed", e);
        }

        writeByteStat = new WriteByteStat();
        // expose stats
        this.baseMetricMonitorStats = new BaseMetricMonitorStats(statsLogger);
    }

    private void initIoStat(Map<DiskLocation, List<DiskIOStat>> ioStats, List<File> directories) {
        if (directories != null) {
            for (File dir : directories) {
                DiskLocation diskLocation = null;
                try {
                    diskLocation = new DiskLocation(dir);
                } catch (IOException e) {
                    LOG.warn("BaseMetricMonitor initIoStat can't find corresponding diskLocation for dir {}", dir);
                }
                List<DiskIOStat> diskIOStats = new ArrayList<>();
                for (int i = 0; i < metricSlideWindowSize; i++) {
                    diskIOStats.add(new DiskIOStat());
                }
                ioStats.put(diskLocation, diskIOStats);
            }
        }
    }

    // start the daemon for disk monitoring
    public void start() {
        LOG.info("Starting BaseMetricMonitor, schedule at fixed rate {}ms", intervalMs);
        this.executor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("BaseMetricMonitorThread")
                .setDaemon(true)
                .build());
        this.checkTask = this.executor.scheduleAtFixedRate(this::updateStat, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    // shutdown disk monitoring daemon
    public void shutdown() {
        LOG.info("Shutting down BaseMetricMonitor");
        if (null != checkTask) {
            if (!checkTask.cancel(true)) {
                LOG.debug("Failed to cancel check task in BaseMetricMonitor");
            }
        }
        if (null != executor) {
            executor.shutdown();
        }
    }

    private void updateStat() {
        Map<String, DiskIOStat> allIoStats = getDiskIoStat();
        Map<String, CPUStat> allCpuStats = getCPUStat();
        DiskIOStat journalIoStat = updateDiskIoStat(allIoStats, journalIoStats, curSliceIdx);
        DiskIOStat ledgerIoStat = updateDiskIoStat(allIoStats, ledgerIoStats, curSliceIdx);
        CPUStat cpuStat = updateCpuStat(allCpuStats);
        long writeBytePerSecond = updateWriteByteStat();
        curSliceIdx = (curSliceIdx + 1) % metricSlideWindowSize;

        // because some base metric need at least one slide round, update base metric after first slide round
        if (updateBaseMetricAfterUpdateStatCount > 0) {
            updateBaseMetricAfterUpdateStatCount -= 1;
            return;
        }

        String oldBaseMetric = baseMetric.toString();
        rwLock.writeLock().lock();
        baseMetric.setJournalIoUtil(journalIoStat.getUtil());
        baseMetric.setLedgerIoUtil(ledgerIoStat.getUtil());
        baseMetric.setCpuUsedRate(cpuStat.getCpuUsedRate());
        baseMetric.setWriteBytePerSecond(writeBytePerSecond);
        rwLock.writeLock().unlock();
        reportStat(baseMetric);
        LOG.debug("BaseMetricMonitor updates base metric from {} to {}", oldBaseMetric, baseMetric);
    }

    private void reportStat(BaseMetric curBaseMetric) {
        // because counter doesn't have set function, use counter add function instead
        int deltaJournalIoUtil, deltaLedgerIoUtil, deltaCpuUsedRate;
        long deltaWriteBytePerSecond;
        if (!reportStatByDelta) {
            deltaJournalIoUtil = curBaseMetric.getJournalIoUtil();
            deltaLedgerIoUtil = curBaseMetric.getLedgerIoUtil();
            deltaCpuUsedRate = curBaseMetric.getCpuUsedRate();
            deltaWriteBytePerSecond = curBaseMetric.getWriteBytePerSecond();
            reportStatByDelta = true;
        } else {
            deltaJournalIoUtil = curBaseMetric.getJournalIoUtil() - baseMetric.getJournalIoUtil();
            deltaLedgerIoUtil = curBaseMetric.getLedgerIoUtil() - baseMetric.getLedgerIoUtil();
            deltaCpuUsedRate = curBaseMetric.getCpuUsedRate() - baseMetric.getCpuUsedRate();
            deltaWriteBytePerSecond = curBaseMetric.getWriteBytePerSecond() - baseMetric.getWriteBytePerSecond();
        }
        baseMetricMonitorStats.getJournalIoUtil().addCount(deltaJournalIoUtil);
        baseMetricMonitorStats.getLedgerIoUtil().addCount(deltaLedgerIoUtil);
        baseMetricMonitorStats.getCpuUsedRate().addCount(deltaCpuUsedRate);
        baseMetricMonitorStats.getWriteBytePerSecond().addCount(deltaWriteBytePerSecond);
    }

    public BaseMetric getBaseMetric() {
        BaseMetric ret = new BaseMetric();
        rwLock.readLock().lock();
        ret.setJournalIoUtil(baseMetric.getJournalIoUtil());
        ret.setLedgerIoUtil(baseMetric.getLedgerIoUtil());
        ret.setCpuUsedRate(baseMetric.getCpuUsedRate());
        ret.setWriteBytePerSecond(baseMetric.getWriteBytePerSecond());
        rwLock.readLock().unlock();
        return ret;
    }

    private DiskIOStat updateDiskIoStat(Map<String, DiskIOStat> allIoStats,
        Map<DiskLocation, List<DiskIOStat>> ioStats, int curIdx) {
        int totalIoUtil = 0;
        List<String> existDisks = new LinkedList<>();
        for (Map.Entry<DiskLocation, List<DiskIOStat>> entry : ioStats.entrySet()) {
            String diskName = entry.getKey().diskName;
            DiskIOStat oldStat = entry.getValue().get(curIdx);
            if (allIoStats.containsKey(diskName)) {
                long oldTotalTicks = oldStat.getLastTotalTicks();
                long oldStatTime = oldStat.getLastStatTime();
                DiskIOStat newStat = allIoStats.get(diskName);
                long newTotalTicks = newStat.getLastTotalTicks();
                long newStatTime = newStat.getLastStatTime();
                oldStat.setLastTotalTicks(newTotalTicks);
                oldStat.setLastStatTime(newStatTime);
                if (oldTotalTicks != 0 && newStatTime > oldStatTime) {
                    int util = (int) ((double) (newTotalTicks - oldTotalTicks) * 100 / (newStatTime - oldStatTime));
                    oldStat.setUtil(util);
                    totalIoUtil += util;
                    existDisks.add(diskName);
                    LOG.debug("{} disk io util:{}", diskName, util);
                    continue;
                }
            }
            // maybe this disk has been umounted.
            // may consider again which value is fair, 100 or -1
            oldStat.setUtil(INVALID);
        }
        DiskIOStat ret = new DiskIOStat();
        if (!existDisks.isEmpty()) {
            // may consider again which stat is fair, average or maximum
            ret.setUtil(totalIoUtil / existDisks.size());
        } else {
            ret.setUtil(INVALID);
        }
        return ret;
    }

    private static final String PROC_DISKSSTATS = "/proc/diskstats";
    private static final Pattern DISK_STAT_FORMAT =
        Pattern.compile("[ \t]*[0-9]*[ \t]*[0-9]*[ \t]*(\\S*)" +
            "[ \t]*[0-9]*[ \t]*[0-9]*[ \t]*[0-9]*" +
            "[ \t]*[0-9]*[ \t]*[0-9]*[ \t]*[0-9]*" +
            "[ \t]*[0-9]*[ \t]*[0-9]*[ \t]*[0-9]*" +
            "[ \t]*([0-9]*)[ \t].*");

    private Map<String, DiskIOStat> getDiskIoStat() {
        Map<String, DiskIOStat> rets = new HashMap<>();
        InputStreamReader fReader = null;
        BufferedReader in = null;
        try {
            fReader = new InputStreamReader(
                new FileInputStream(PROC_DISKSSTATS), Charset.forName("UTF-8"));
            in = new BufferedReader(fReader);
        } catch (FileNotFoundException f) {
            // shouldn't happen....
            LOG.warn("BaseMetricMonitor gets FileNotFoundException while getDiskIoStat", f);
            return rets;
        }
        try {
            Matcher mat = null;
            String str = in.readLine();
            long statTime = System.currentTimeMillis();
            while (str != null) {
                mat = DISK_STAT_FORMAT.matcher(str);
                if (mat.find()) {
                    String diskName = mat.group(1);
                    long totalTicks = Long.parseLong(mat.group(2));
                    LOG.debug("{} totalTicks:{}", str, totalTicks);
                    DiskIOStat stat = new DiskIOStat();
                    stat.setLastTotalTicks(totalTicks);
                    stat.setLastStatTime(statTime);
                    rets.put(diskName, stat);
                }
                str = in.readLine();
                statTime = System.currentTimeMillis();
            }
        } catch (IOException e) {
            LOG.warn("BaseMetricMonitor gets exception while getDiskIoStat", e);
        }
        return rets;
    }

    private static final String PROC = "/proc/%s/status";
    private static final Pattern PID_STATUS_CPU_FORMAT =
        Pattern.compile("Cpus_allowed_list:[ \t]*(\\S+)");
    private static final String CPU = "cpu";

    private String getPid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        String[] split = name.split("@");
        return split[0];
    }

    private void getUsedCPU(Map<String, CPUStat> cpuStats) throws IOException {
        InputStreamReader fReader = null;
        BufferedReader in = null;
        try {
            fReader = new InputStreamReader(
                new FileInputStream(String.format(PROC, getPid())), Charset.forName("UTF-8"));
            in = new BufferedReader(fReader);
        } catch (FileNotFoundException f) {
            // shouldn't happen....
            LOG.warn("BaseMetricMonitor gets FileNotFoundException while getUsedCPU", f);
            throw f;
        }
        try {
            Matcher mat = null;
            String str = in.readLine();
            while (str != null) {
                mat = PID_STATUS_CPU_FORMAT.matcher(str);
                if (mat.find()) {
                    String cpu = mat.group(1);
                    LOG.debug("{} cpuName:{}", str, cpu);
                    String[] splits = cpu.split(",");
                    for (int i = 0; i < splits.length; i++) {
                        String subSplits = splits[i];
                        String[] subSplit = subSplits.split("-");
                        if (subSplit.length == 1) {
                            String cpuName = CPU + subSplit[0];
                            cpuStats.put(cpuName, new CPUStat());
                        } else if (subSplit.length == 2) {
                            int startIdx = Integer.parseInt(subSplit[0]);
                            int endIdx = Integer.parseInt(subSplit[1]);
                            for (int j = startIdx; j <= endIdx; j++) {
                                String cpuName = CPU + j;
                                cpuStats.put(cpuName, new CPUStat());
                            }
                        }
                    }
                    break;
                }
                str = in.readLine();
            }
        } catch (IOException e) {
            LOG.warn("BaseMetricMonitor gets exception while getUsedCPU", e);
            throw e;
        }
        if (cpuStats.isEmpty()) {
            cpuStats.put(CPU, new CPUStat());
        }
    }
    private static final String PROC_STAT = "/proc/stat";
    private static final Pattern PROC_STAT_CPU_FORMAT =
        Pattern.compile("(cpu[0-9]*)[ \t]+([0-9]+)[ \t]+([0-9]+)[ \t]+([0-9]+)" +
            "[ \t]+([0-9]+)[ \t]+([0-9]+)");

    private Map<String, CPUStat> getCPUStat() {
        Map<String, CPUStat> rets = new HashMap<>();
        InputStreamReader fReader = null;
        BufferedReader in = null;
        try {
            fReader = new InputStreamReader(
                new FileInputStream(PROC_STAT), Charset.forName("UTF-8"));
            in = new BufferedReader(fReader);
        } catch (FileNotFoundException f) {
            // shouldn't happen....
            LOG.warn("BaseMetricMonitor gets FileNotFoundException while getCPUStat", f);
            return rets;
        }
        try {
            Matcher mat = null;
            String str = in.readLine();
            while (str != null) {
                mat = PROC_STAT_CPU_FORMAT.matcher(str);
                if (mat.find()) {
                    long totalTime = 0;
                    String cpuName = mat.group(1);
                    for (int i = 2; i <= mat.groupCount(); i++) {
                        totalTime += Long.parseLong(mat.group(i));
                    }
                    long freeTime = Long.parseLong(mat.group(5));
                    LOG.debug("{} cpuName:{} totalTime:{} freeTime:{}", str, cpuName, totalTime, freeTime);
                    CPUStat stat = new CPUStat();
                    stat.setLastTotalTime(totalTime);
                    stat.setLastFreeTime(freeTime);
                    rets.put(cpuName, stat);
                }
                str = in.readLine();
            }
        } catch (IOException e) {
            LOG.warn("BaseMetricMonitor gets exception while getCPUStat", e);
        }
        return rets;
    }

    private CPUStat updateCpuStat(Map<String, CPUStat> allCpuStats) {
        int totalCpuUsedRate = 0;
        int existCpuNum = 0;
        for (Map.Entry<String, CPUStat> entry : cpuStats.entrySet()) {
            String cpuName = entry.getKey();
            CPUStat oldStat = entry.getValue();
            if (allCpuStats.containsKey(cpuName)) {
                long oldTotalTime = oldStat.getLastTotalTime();
                long oldFreeTime = oldStat.getLastFreeTime();
                CPUStat newStat = allCpuStats.get(cpuName);
                long newTotalTime = newStat.getLastTotalTime();
                long newFreeTime = newStat.getLastFreeTime();
                oldStat.setLastFreeTime(newFreeTime);
                oldStat.setLastTotalTime(newTotalTime);
                if (oldTotalTime != 0 && newTotalTime > oldTotalTime) {
                    int cpuUsedRate = (int) (100 - ((newFreeTime - oldFreeTime) * 100 / (newTotalTime - oldTotalTime)));
                    oldStat.setCpuUsedRate(cpuUsedRate);
                    totalCpuUsedRate += cpuUsedRate;
                    ++existCpuNum;
                    LOG.debug("{} cpu used rate:{}", cpuName, cpuUsedRate);
                    continue;
                }
            }
            // may consider again which value is fair, 100 or -1
            oldStat.setCpuUsedRate(INVALID);
        }
        CPUStat ret = new CPUStat();
        if (existCpuNum > 0) {
            // may consider again which stat is fair, average or maximum
            ret.setCpuUsedRate(totalCpuUsedRate / existCpuNum);
        } else {
            ret.setCpuUsedRate(INVALID);
        }
        return ret;
    }

    private long updateWriteByteStat() {
        long writeBytePerSecond = 0;
        long oldWriteByte = writeByteStat.getLastWriteByte();
        long oldStatTime = writeByteStat.getLastStatTime();
        long newWriteByte = bookieStats.getWriteBytes().get();
        long newStatTime = System.currentTimeMillis();
        if (oldWriteByte != 0 && newStatTime > oldStatTime) {
            writeBytePerSecond = (newWriteByte - oldWriteByte) * TimeUnit.SECONDS.toMillis(1) / (newStatTime - oldStatTime);
        }
        writeByteStat.setLastWriteByte(newWriteByte);
        writeByteStat.setLastStatTime(newStatTime);
        writeByteStat.setWriteBytePerSecond(writeBytePerSecond);
        LOG.debug("write byte per second:{}", writeBytePerSecond);
        return writeBytePerSecond;
    }
}