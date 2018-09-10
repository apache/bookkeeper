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
package org.apache.bookkeeper.client;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

/**
 * Define a policy for speculative request execution.
 *
 * <p>The implementation can define its execution policy. For example, when to issue speculative requests
 * and how many speculative requests to issue.
 *
 * @since 4.5
 */
public interface SpeculativeRequestExecutionPolicy {

    /**
     * Initialize the speculative request execution policy and initiate requests.
     *
     * @param scheduler The scheduler service to issue the speculative request
     * @param requestExectuor The executor is used to issue the actual speculative requests
     * @return ScheduledFuture, in case caller needs to cancel it.
     */
    ScheduledFuture<?> initiateSpeculativeRequest(ScheduledExecutorService scheduler,
            SpeculativeRequestExecutor requestExectuor);
}
