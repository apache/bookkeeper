package org.apache.bookkeeper.http.service;

import org.apache.bookkeeper.http.HttpServer;

public class HeartbeatService implements Service {
    @Override
    public ServiceResponse handle(ServiceRequest request) {
        return new ServiceResponse("OK\n", HttpServer.StatusCode.OK);
    }
}
