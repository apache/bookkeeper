package org.apache.bookkeeper.http.service;

public interface Service {

    public ServiceResponse handle(ServiceRequest request);
}
