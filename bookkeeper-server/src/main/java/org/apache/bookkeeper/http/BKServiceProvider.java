package org.apache.bookkeeper.http;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.service.HeartbeatService;
import org.apache.bookkeeper.http.service.Service;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.Auditor;
import org.apache.bookkeeper.replication.AutoRecoveryMain;

public class BKServiceProvider implements ServiceProvider {

    private BookieServer bookieServer;
    private AutoRecoveryMain autoRecovery;
    private ServerConfiguration conf;

    @Override
    public Service provideHeartbeatService() {
        return new HeartbeatService();
    }

    @Override
    public Service provideConfigurationService() {
        ServerConfiguration configuration = getConf();
        if (configuration == null) {
            return NullServiceProvider.NULL_SERVICE;
        }
        return new ConfigurationService(configuration);
    }

    public BKServiceProvider setBookieServer(BookieServer bookieServer) {
        this.bookieServer = bookieServer;
        return this;
    }

    public BKServiceProvider setAutoRecovery(AutoRecoveryMain autoRecovery) {
        this.autoRecovery = autoRecovery;
        return this;
    }

    public BKServiceProvider setConf(ServerConfiguration conf) {
        this.conf = conf;
        return this;
    }

    private ServerConfiguration getConf() {
        return conf;
    }

    private Auditor getAuditor() {
        return autoRecovery == null ? null : autoRecovery.getAuditor();
    }

}
