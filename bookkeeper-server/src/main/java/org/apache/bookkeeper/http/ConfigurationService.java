package org.apache.bookkeeper.http;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.service.Service;
import org.apache.bookkeeper.http.service.ServiceRequest;
import org.apache.bookkeeper.http.service.ServiceResponse;
import org.json.JSONObject;

public class ConfigurationService implements Service {

    protected ServerConfiguration conf;

    public ConfigurationService(ServerConfiguration conf) {
        assert conf != null;
        this.conf = conf;
    }

    @Override
    public ServiceResponse handle(ServiceRequest request) {
        ServiceResponse response = new ServiceResponse();
        Map configMap = toMap(conf);
        JSONObject jsonResponse = new JSONObject();
        jsonResponse.put("server_config", new JSONObject(configMap));
        response.setBody(jsonResponse.toString(2));
        return response;
    }

    private Map toMap(ServerConfiguration conf) {
        Map<String, Object> configMap = new HashMap<>();
        Iterator iterator = conf.getKeys();
        while (iterator.hasNext()) {
            String key = iterator.next().toString();
            List values = conf.getList(key);
            if (values.size() >= 0) {
                configMap.put(key, values.get(0));
            }
        }
        return configMap;
    }
}
