package org.apache.bookkeeper.http.service;

import org.apache.bookkeeper.http.HttpServer;

public class ServiceResponse {
    private String body;
    private HttpServer.StatusCode code = HttpServer.StatusCode.OK;

    public ServiceResponse() {}

    public ServiceResponse(String body, HttpServer.StatusCode code) {
        this.body = body;
        this.code = code;
    }

    public String getBody() {
        return body;
    }

    public int getStatusCode() {
        return code.getValue();
    }

    public ServiceResponse setBody(String body) {
        this.body = body;
        return this;
    }

    public ServiceResponse setCode(HttpServer.StatusCode code) {
        this.code = code;
        return this;
    }
}
