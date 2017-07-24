package org.apache.bookkeeper.http.service;

import java.util.HashMap;
import java.util.Map;

import org.apache.bookkeeper.http.HttpServer;

public class ServiceRequest {
    private String body;
    private HttpServer.Method method = HttpServer.Method.GET;
    private Map params = new HashMap();

    public ServiceRequest() {}

    public ServiceRequest(String body, HttpServer.Method method, Map params) {
        this.body = body;
        this.method = method;
        this.params = params;
    }

    public String getBody() {
        return body;
    }

    public ServiceRequest setBody(String body) {
        this.body = body;
        return this;
    }

    public HttpServer.Method getMethod() {
        return method;
    }

    public ServiceRequest setMethod(HttpServer.Method method) {
        this.method = method;
        return this;
    }

    public Map getParams() {
        return params;
    }

    public ServiceRequest setParams(Map params) {
        this.params = params;
        return this;
    }
}
