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

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.feature.SettableFeatureProvider;

class ClientInternalConf {
    final Feature disableEnsembleChangeFeature;
    final boolean delayEnsembleChange;

    final Optional<SpeculativeRequestExecutionPolicy> readSpeculativeRequestPolicy;
    final Optional<SpeculativeRequestExecutionPolicy> readLACSpeculativeRequestPolicy;

    final int explicitLacInterval;
    final long waitForWriteSetMs;
    final long addEntryQuorumTimeoutNanos;
    final boolean enableParallelRecoveryRead;
    final boolean enableReorderReadSequence;
    final boolean enableStickyReads;
    final int recoveryReadBatchSize;
    final int throttleValue;
    final int bookieFailureHistoryExpirationMSec;
    final int maxAllowedEnsembleChanges;
    final long timeoutMonitorIntervalSec;
    final boolean enableBookieFailureTracking;
    final boolean useV2WireProtocol;
    final boolean enforceMinNumFaultDomainsForWrite;
    final boolean batchReadEnabled;
    final int nettyMaxFrameSizeBytes;

    static ClientInternalConf defaultValues() {
        return fromConfig(new ClientConfiguration());
    }

    static ClientInternalConf fromConfig(ClientConfiguration conf) {
        return fromConfigAndFeatureProvider(conf, SettableFeatureProvider.DISABLE_ALL);
    }

    static ClientInternalConf fromConfigAndFeatureProvider(ClientConfiguration conf,
                                                           FeatureProvider featureProvider) {
        return new ClientInternalConf(conf, featureProvider);
    }

    private ClientInternalConf(ClientConfiguration conf,
                               FeatureProvider featureProvider) {
        this.explicitLacInterval = conf.getExplictLacInterval();
        this.enableReorderReadSequence = conf.isReorderReadSequenceEnabled();
        this.enableParallelRecoveryRead = conf.getEnableParallelRecoveryRead();
        this.recoveryReadBatchSize = conf.getRecoveryReadBatchSize();
        this.waitForWriteSetMs = conf.getWaitTimeoutOnBackpressureMillis();
        this.addEntryQuorumTimeoutNanos = TimeUnit.SECONDS.toNanos(conf.getAddEntryQuorumTimeout());
        this.throttleValue = conf.getThrottleValue();
        this.bookieFailureHistoryExpirationMSec = conf.getBookieFailureHistoryExpirationMSec();
        this.batchReadEnabled = conf.isBatchReadEnabled();
        this.nettyMaxFrameSizeBytes = conf.getNettyMaxFrameSizeBytes();
        this.disableEnsembleChangeFeature = featureProvider.getFeature(conf.getDisableEnsembleChangeFeatureName());
        this.delayEnsembleChange = conf.getDelayEnsembleChange();
        this.maxAllowedEnsembleChanges = conf.getMaxAllowedEnsembleChanges();
        this.timeoutMonitorIntervalSec = conf.getTimeoutMonitorIntervalSec();
        this.enableBookieFailureTracking = conf.getEnableBookieFailureTracking();
        this.useV2WireProtocol = conf.getUseV2WireProtocol();
        this.enableStickyReads = conf.isStickyReadsEnabled();
        this.enforceMinNumFaultDomainsForWrite = conf.getEnforceMinNumFaultDomainsForWrite();

        if (conf.getFirstSpeculativeReadTimeout() > 0) {
            this.readSpeculativeRequestPolicy =
                    Optional.of(new DefaultSpeculativeRequestExecutionPolicy(
                                        conf.getFirstSpeculativeReadTimeout(),
                                        conf.getMaxSpeculativeReadTimeout(),
                                        conf.getSpeculativeReadTimeoutBackoffMultiplier()));
        } else {
            this.readSpeculativeRequestPolicy = Optional.<SpeculativeRequestExecutionPolicy>empty();
        }
        if (conf.getFirstSpeculativeReadLACTimeout() > 0) {
            this.readLACSpeculativeRequestPolicy =
                    Optional.of(new DefaultSpeculativeRequestExecutionPolicy(
                        conf.getFirstSpeculativeReadLACTimeout(),
                        conf.getMaxSpeculativeReadLACTimeout(),
                        conf.getSpeculativeReadLACTimeoutBackoffMultiplier()));
        } else {
            this.readLACSpeculativeRequestPolicy = Optional.<SpeculativeRequestExecutionPolicy>empty();
        }
    }
}
