package org.apache.bookkeeper.http;

import org.apache.bookkeeper.http.service.Service;
import org.apache.bookkeeper.http.service.ServiceRequest;
import org.apache.bookkeeper.http.service.ServiceResponse;

public class NullServiceProvider implements ServiceProvider {

    private static final NullServiceProvider NULL_SERVICE_PROVIDER = new NullServiceProvider();

    public static final Service NULL_SERVICE = new NullService();

    static class NullService implements Service {
        @Override
        public ServiceResponse handle(ServiceRequest request) {
            return new ServiceResponse().setCode(HttpServer.StatusCode.NOT_FOUND);
        }
    }

    @Override
    public Service provideHeartbeatService() {
        return NULL_SERVICE;
    }

    @Override
    public Service provideConfigurationService() {
        return NULL_SERVICE;
    }

    public static NullServiceProvider getInstance() {
        return NULL_SERVICE_PROVIDER;
    }
}
