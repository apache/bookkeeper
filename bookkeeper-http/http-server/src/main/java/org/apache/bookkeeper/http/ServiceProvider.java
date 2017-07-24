package org.apache.bookkeeper.http;

import org.apache.bookkeeper.http.service.Service;

public interface ServiceProvider {

    Service provideHeartbeatService();

    Service provideConfigurationService();
}
