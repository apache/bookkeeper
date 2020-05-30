/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.metastore;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.metastore.MetastoreTable.ALL_FIELDS;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.primitives.UnsignedBytes;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A metastore value.
 */
public class Value {
    private static final Comparator<byte[]> comparator =
        UnsignedBytes.lexicographicalComparator();

    protected Map<String, byte[]> fields;

    public Value() {
        fields = new HashMap<String, byte[]>();
    }

    public Value(Value v) {
        fields = new HashMap<String, byte[]>(v.fields);
    }

    public byte[] getField(String field) {
        return fields.get(field);
    }

    public Value setField(String field, byte[] data) {
        fields.put(field, data);
        return this;
    }

    public Value clearFields() {
        fields.clear();
        return this;
    }

    public Set<String> getFields() {
        return fields.keySet();
    }

    public Map<String, byte[]> getFieldsMap() {
        return Collections.unmodifiableMap(fields);
    }

    /**
     * Select parts of fields.
     *
     * @param fields
     *            Parts of fields
     * @return new value with specified fields
     */
    public Value project(Set<String> fields) {
        if (ALL_FIELDS == fields) {
            return new Value(this);
        }
        Value v = new Value();
        for (String f : fields) {
            byte[] data = this.fields.get(f);
            v.setField(f, data);
        }
        return v;
    }

    @Override
    public int hashCode() {
        HashFunction hf = Hashing.murmur3_32();
        Hasher hc = hf.newHasher();
        for (String key : fields.keySet()) {
            hc.putString(key, Charset.defaultCharset());
        }
        return hc.hash().asInt();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Value)) {
            return false;
        }
        Value other = (Value) o;
        if (fields.size() != other.fields.size()) {
            return false;
        }
        for (Map.Entry<String, byte[]> entry : fields.entrySet()) {
            String f = entry.getKey();
            byte[] v1 = entry.getValue();
            byte[] v2 = other.fields.get(f);
            if (0 != comparator.compare(v1, v2)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Merge other value.
     *
     * @param other
     *          Other Value
     */
    public Value merge(Value other) {
        for (Map.Entry<String, byte[]> entry : other.fields.entrySet()) {
            if (null == entry.getValue()) {
                fields.remove(entry.getKey());
            } else {
                fields.put(entry.getKey(), entry.getValue());
            }
        }
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Map.Entry<String, byte[]> entry : fields.entrySet()) {
            String f = entry.getKey();
            if (null == f) {
                f = "NULL";
            }
            String value;
            if (null == entry.getValue()) {
                value = "NONE";
            } else {
                value = new String(entry.getValue(), UTF_8);
            }
            sb.append("('").append(f).append("'=").append(value).append(")");
        }
        sb.append("]");
        return sb.toString();
    }
}
