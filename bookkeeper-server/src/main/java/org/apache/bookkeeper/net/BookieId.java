/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.net;

import java.util.Objects;

/**
 * This is an identifier for a BookieID.
 */
public final class BookieId {

    private final String id;

    private BookieId(String id) {
        validateBookieId(id);
        this.id = id;
    }

    /**
     * Returns the serialized version of this object.
     * @return the bookieId
     */
    @Override
    public String toString() {
        return id;
    }

    /**
     * Parses the given serialized representation of a BookieId.
     * @param serialized
     * @return the parsed BookieId
     */
    public static BookieId parse(String serialized) {
       return new BookieId(serialized);
    }

    public String getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return this.id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final BookieId other = (BookieId) obj;
        if (!Objects.equals(this.id, other.id)) {
            return false;
        }
        return true;
    }

    private static void validateBookieId(String id) {
        Objects.requireNonNull(id, "BookieId cannot be null");
        if (!(id.matches("[a-zA-Z0-9:-_.\\-]+"))
                || "readonly".equalsIgnoreCase(id)) {
            throw new IllegalArgumentException("BookieId " + id + " is not valid");
        }
    }

}
