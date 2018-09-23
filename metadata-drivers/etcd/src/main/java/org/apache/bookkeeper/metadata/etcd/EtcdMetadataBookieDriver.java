/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.metadata.etcd;

import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.discover.RegistrationManager.RegistrationListener;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Etcd based metadata bookie driver.
 */
@Slf4j
public class EtcdMetadataBookieDriver extends EtcdMetadataDriverBase implements MetadataBookieDriver {

    // register myself
    static {
        MetadataDrivers.registerBookieDriver(
            SCHEME, EtcdMetadataBookieDriver.class);
        log.info("Registered etcd metadata bookie driver");
    }

    ServerConfiguration conf;
    EtcdBookieRegister bkRegister;
    RegistrationManager regMgr;
    RegistrationListener listener;

    @Override
    public synchronized MetadataBookieDriver initialize(ServerConfiguration conf,
                                                        RegistrationListener listener,
                                                        StatsLogger statsLogger)
            throws MetadataException {
        super.initialize(conf, statsLogger);
        this.conf = conf;
        this.listener = listener;
        this.statsLogger = statsLogger;
        return null;
    }

    @Override
    public synchronized RegistrationManager getRegistrationManager() {
        if (null == bkRegister) {
            bkRegister = new EtcdBookieRegister(
                client.getLeaseClient(),
                TimeUnit.MILLISECONDS.toSeconds(conf.getZkTimeout()),
                listener
            ).start();
        }
        if (null == regMgr) {
            regMgr = new EtcdRegistrationManager(
                client,
                keyPrefix,
                bkRegister
            );
        }
        return regMgr;
    }

    @Override
    public void close() {
        RegistrationManager rmToClose;
        EtcdBookieRegister bkRegisterToClose;
        synchronized (this) {
            rmToClose = regMgr;
            regMgr = null;
            bkRegisterToClose = bkRegister;
            bkRegister = null;
        }
        if (null != rmToClose) {
            rmToClose.close();
        }
        if (null != bkRegisterToClose) {
            bkRegisterToClose.close();
        }
        super.close();
    }
}
