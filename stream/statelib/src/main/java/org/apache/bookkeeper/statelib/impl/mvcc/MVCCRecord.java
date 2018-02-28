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

package org.apache.bookkeeper.statelib.impl.mvcc;

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import java.util.function.Predicate;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.api.kv.impl.result.KeyValueFactory;
import org.apache.bookkeeper.api.kv.impl.result.KeyValueImpl;
import org.apache.bookkeeper.api.kv.options.RangeOption;
import org.apache.bookkeeper.common.coder.Coder;
import org.apache.bookkeeper.common.util.Recycled;
import org.apache.bookkeeper.stream.proto.kv.store.ValueType;

/**
 * An object represents the mvcc metdata and value for a given key.
 */
@Data
@Setter
@Getter
public class MVCCRecord implements Recycled, Predicate<RangeOption<?>> {

    public static MVCCRecord newRecord() {
        return RECYCLER.get();
    }

    private static final Recycler<MVCCRecord> RECYCLER = new Recycler<MVCCRecord>() {
        @Override
        protected MVCCRecord newObject(Handle<MVCCRecord> handle) {
            return new MVCCRecord(handle);
        }
    };

    private ByteBuf value;
    private long number;
    private final Recycler.Handle<MVCCRecord> handle;
    private long createRev;
    private long modRev;
    private long version;
    private ValueType valueType = ValueType.BYTES;

    private MVCCRecord(Recycler.Handle<MVCCRecord> handle) {
        this.handle = handle;
    }

    public MVCCRecord duplicate() {
        MVCCRecord record = newRecord();
        record.createRev = createRev;
        record.modRev = modRev;
        record.version = version;
        record.valueType = valueType;
        record.value = value.retainedSlice();
        record.number = number;
        return record;
    }

    public int compareModRev(long revision) {
        return Long.compare(modRev, revision);
    }

    public int compareCreateRev(long revision) {
        return Long.compare(createRev, revision);
    }

    public int compareVersion(long version) {
        return Long.compare(this.version, version);
    }

    public void setValue(ByteBuf buf, ValueType valueType) {
        if (null != value) {
            value.release();
        }
        this.value = buf;
        this.valueType = valueType;
        if (ValueType.NUMBER == valueType) {
            this.number = buf.getLong(0);
        }
    }

    private void reset() {
        if (null != value) {
            value.release();
            value = null;
        }
        modRev = -1L;
        createRev = -1L;
        version = -1L;
        number = -1L;
        valueType = ValueType.BYTES;
    }

    @Override
    public void recycle() {
        reset();
        handle.recycle(this);
    }

    <K, V> KeyValueImpl<K, V> asKVRecord(KeyValueFactory<K, V> recordFactory,
                                         K key,
                                         Coder<V> valCoder) {
        return recordFactory.newKv()
            .key(key)
            .value(valCoder.decode(value))
            .createRevision(createRev)
            .modifiedRevision(modRev)
            .version(version)
            .isNumber(ValueType.NUMBER == valueType)
            .numberValue(number);
    }

    @Override
    public boolean test(RangeOption<?> rangeOption) {
        if (null == rangeOption) {
            return true;
        }
        return createRev >= rangeOption.minCreateRev()
            && createRev <= rangeOption.maxCreateRev()
            && modRev >= rangeOption.minModRev()
            && modRev <= rangeOption.maxModRev();
    }
}
