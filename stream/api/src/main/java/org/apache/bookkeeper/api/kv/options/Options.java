/*
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
package org.apache.bookkeeper.api.kv.options;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * A collection of default options.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Options {

    @SuppressWarnings("rawtypes")
    private static final DeleteOption DEFAULT_DELETE_OPTION = new DeleteOption() {
        @Override
        public Object endKey() {
            return null;
        }

        @Override
        public boolean prevKv() {
            return false;
        }

        @Override
        public void close() {
        }

        @Override
        public String toString() {
            return "DeleteOption(prevKv=false)";
        }
    };

    @SuppressWarnings("rawtypes")
    private static final DeleteOption DELETE_AND_GET_OPTION = new DeleteOption() {
        @Override
        public Object endKey() {
            return null;
        }

        @Override
        public boolean prevKv() {
            return true;
        }

        @Override
        public void close() {
        }

        @Override
        public String toString() {
            return "DeleteOption(prevKv=true)";
        }
    };

    @SuppressWarnings("rawtypes")
    private static final RangeOption GET_OPTION = new RangeOption() {
        @Override
        public long limit() {
            return 1;
        }

        @Override
        public long minModRev() {
            return Long.MIN_VALUE;
        }

        @Override
        public long maxModRev() {
            return Long.MAX_VALUE;
        }

        @Override
        public long minCreateRev() {
            return Long.MIN_VALUE;
        }

        @Override
        public long maxCreateRev() {
            return Long.MAX_VALUE;
        }

        @Override
        public boolean keysOnly() {
            return false;
        }

        @Override
        public boolean countOnly() {
            return false;
        }

        @Override
        public Object endKey() {
            return null;
        }

        @Override
        public void close() {
        }

        @Override
        public String toString() {
            return "RangeOption(limit=1)";
        }
    };

    @SuppressWarnings("rawtypes")
    private static final PutOption BLIND_PUT_OPTION = new PutOption() {

        @Override
        public void close() {
        }

        @Override
        public boolean prevKv() {
            return false;
        }

        @Override
        public String toString() {
            return "PutOption(prevKv=false)";
        }

    };

    @SuppressWarnings("rawtypes")
    private static final PutOption PUT_AND_GET_OPTION = new PutOption() {

        @Override
        public void close() {
        }

        @Override
        public boolean prevKv() {
            return true;
        }

        @Override
        public String toString() {
            return "PutOption(prevKv=true)";
        }

    };

    @SuppressWarnings("rawtypes")
    private static final IncrementOption BLIND_INCREMENT_OPTION = new IncrementOption() {
        @Override
        public boolean getTotal() {
            return false;
        }

        @Override
        public void close() {
        }

        @Override
        public String toString() {
            return "IncrementOption(getTotal=false)";
        }
    };

    @SuppressWarnings("rawtypes")
    private static final IncrementOption INCREMENT_AND_GET_OPTION = new IncrementOption() {
        @Override
        public boolean getTotal() {
            return true;
        }

        @Override
        public void close() {
        }

        @Override
        public String toString() {
            return "IncrementOption(getTotal=true)";
        }
    };

    @SuppressWarnings("unchecked")
    public static <K> PutOption<K> blindPut() {
        return (PutOption<K>) BLIND_PUT_OPTION;
    }

    @SuppressWarnings("unchecked")
    public static <K> IncrementOption<K> blindIncrement() {
        return (IncrementOption<K>) BLIND_INCREMENT_OPTION;
    }

    @SuppressWarnings("unchecked")
    public static <K> IncrementOption<K> incrementAndGet() {
        return (IncrementOption<K>) INCREMENT_AND_GET_OPTION;
    }

    @SuppressWarnings("unchecked")
    public static <K> PutOption<K> putAndGet() {
        return (PutOption<K>) PUT_AND_GET_OPTION;
    }

    @SuppressWarnings("unchecked")
    public static <K> RangeOption<K> get() {
        return (RangeOption<K>) GET_OPTION;
    }

    @SuppressWarnings("unchecked")
    public static <K> DeleteOption<K> delete() {
        return (DeleteOption<K>) DEFAULT_DELETE_OPTION;
    }

    @SuppressWarnings("unchecked")
    public static <K> DeleteOption<K> deleteAndGet() {
        return (DeleteOption<K>) DELETE_AND_GET_OPTION;
    }

}
