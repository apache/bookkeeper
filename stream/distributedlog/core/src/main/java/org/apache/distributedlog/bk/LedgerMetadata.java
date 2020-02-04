/*
 * Copyright 2020 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.distributedlog.bk;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Ledger metadata.
 */
public final class LedgerMetadata {

    private String application;
    private String component;
    private Map<String, String> customMetadata;

    public void setApplication(String application) {
        this.application = application;
    }

    public void setComponent(String component) {
        this.component = component;
    }

    public void addCustomMetadata(String key, String value) {
        if (key == null || "".equals(key.trim())) {
            throw new IllegalArgumentException("Metadata key cant be empty");
        }
        if (value == null || "".equals(value.trim())) {
            throw new IllegalArgumentException("Metadata value cant be empty");
        }

        if (customMetadata == null) {
            customMetadata = new HashMap<>();
        }

        customMetadata.put(key, value);
    }

    public Map<String, byte[]> getMetadata() {
        Map<String, byte[]> meta = new HashMap<>();
        if (application != null) {
            meta.put("application", application.getBytes(StandardCharsets.UTF_8));
        }
        if (component != null) {
            meta.put("component", component.getBytes(StandardCharsets.UTF_8));
        }

        if (customMetadata != null) {
            for (Map.Entry<String, String> e : customMetadata.entrySet()) {
                String value = e.getValue();
                meta.put(e.getKey(), value.getBytes(StandardCharsets.UTF_8));
            }
        }

        return meta;
    }
}
