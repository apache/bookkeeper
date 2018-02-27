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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A quartz job used for cron based compaction.
 */
public class CronBasedCompactionJob implements Job {
    private static final Logger LOG = LoggerFactory.getLogger(CronBasedCompactionJob.class);
    public GarbageCollectorThread garbageCollectorThread;
    @Override
    public void execute(JobExecutionContext context)
            throws JobExecutionException {
        JobDataMap dataMap = context.getJobDetail().getJobDataMap();
        garbageCollectorThread = (GarbageCollectorThread) dataMap.get("gcThread");
        garbageCollectorThread.safeRun();
        if (LOG.isDebugEnabled()) {
            String jobName = context.getJobDetail().getKey().getName();
            SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH hour mm minute ss second");
            String jobRunTime = dateFormat.format(Calendar.getInstance().getTime());
            LOG.debug("Job {} executed on {}", jobName, jobRunTime);
        }
    }
}
