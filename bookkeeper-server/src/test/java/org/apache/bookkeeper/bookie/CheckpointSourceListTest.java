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
package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import java.io.IOException;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.junit.Test;

/**
 * Tests for {@link CheckpointSourceList}.
 */
public class CheckpointSourceListTest {

    private final TestCheckpointSource firstSource = new TestCheckpointSource();
    private final TestCheckpointSource secondSource = new TestCheckpointSource();
    private final CheckpointSourceList checkpointSource =
            new CheckpointSourceList(Lists.newArrayList(firstSource, secondSource));

    @Test
    public void testIncomparableCheckpointsAreNotAfterEachOther() {
        Checkpoint first = newCheckpoint(2, 1);
        Checkpoint second = newCheckpoint(1, 2);

        assertTrue(first.compareTo(second) > 0);
        assertFalse(first.isAfter(second));
        assertFalse(second.isAfter(first));
    }

    @Test
    public void testCheckpointIsAfterOnlyWhenAllComponentsAreCovered() {
        Checkpoint requested = newCheckpoint(1, 2);
        Checkpoint after = newCheckpoint(2, 2);
        Checkpoint equal = newCheckpoint(1, 2);
        Checkpoint before = newCheckpoint(1, 1);

        assertTrue(after.isAfter(requested));
        assertFalse(equal.isAfter(requested));
        assertFalse(before.isAfter(requested));
    }

    @Test
    public void testMinAndMaxCheckpoints() {
        Checkpoint checkpoint = newCheckpoint(1, 1);

        assertTrue(checkpoint.isAfter(Checkpoint.MIN));
        assertFalse(checkpoint.isAfter(Checkpoint.MAX));
        assertTrue(Checkpoint.MAX.isAfter(checkpoint));
        assertFalse(Checkpoint.MIN.isAfter(checkpoint));
        assertFalse(Checkpoint.MAX.isAfter(Checkpoint.MAX));
        assertFalse(Checkpoint.MIN.isAfter(Checkpoint.MIN));
    }

    @Test
    public void testSingleSourceKeepsScalarOrdering() {
        TestCheckpointSource source = new TestCheckpointSource();
        CheckpointSourceList singleSource = new CheckpointSourceList(Lists.newArrayList(source));

        source.setValue(1);
        Checkpoint first = singleSource.newCheckpoint();
        source.setValue(2);
        Checkpoint second = singleSource.newCheckpoint();

        assertTrue(second.isAfter(first));
        assertFalse(first.isAfter(second));
        assertFalse(first.isAfter(first));
    }

    private Checkpoint newCheckpoint(long first, long second) {
        firstSource.setValue(first);
        secondSource.setValue(second);
        return checkpointSource.newCheckpoint();
    }

    private static final class TestCheckpointSource implements CheckpointSource {
        private long value;

        void setValue(long value) {
            this.value = value;
        }

        @Override
        public Checkpoint newCheckpoint() {
            return new TestCheckpoint(value);
        }

        @Override
        public void checkpointComplete(Checkpoint checkpoint, boolean compact) throws IOException {
            // No-op
        }
    }

    private static final class TestCheckpoint implements Checkpoint {
        private final long value;

        TestCheckpoint(long value) {
            this.value = value;
        }

        @Override
        public int compareTo(Checkpoint other) {
            if (other == Checkpoint.MAX) {
                return -1;
            } else if (other == Checkpoint.MIN) {
                return 1;
            }
            return Long.compare(value, ((TestCheckpoint) other).value);
        }
    }
}
