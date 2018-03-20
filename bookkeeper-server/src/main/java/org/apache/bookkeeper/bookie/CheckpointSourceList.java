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
package org.apache.bookkeeper.bookie;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;

/**
 * A {@code CheckpointSourceList} manages a list of {@link CheckpointSource}s.
 */
public class CheckpointSourceList implements CheckpointSource {

    private final List<? extends CheckpointSource> checkpointSourcesList;

    public CheckpointSourceList(List<? extends CheckpointSource> checkpointSourcesList) {
        this.checkpointSourcesList = checkpointSourcesList;
    }

    @Override
    public Checkpoint newCheckpoint() {
        return new CheckpointList(this);
    }

    @Override
    public void checkpointComplete(Checkpoint checkpoint, boolean compact) throws IOException {
        if (checkpoint == Checkpoint.MAX || checkpoint == Checkpoint.MIN) {
            return;
        }

        checkArgument(checkpoint instanceof CheckpointList);
        CheckpointList checkpointList = (CheckpointList) checkpoint;

        checkArgument(checkpointList.source == this);
        checkpointList.checkpointComplete(compact);
    }

    private static class CheckpointList implements Checkpoint {
        private final CheckpointSourceList source;
        private final List<Checkpoint> checkpoints;

        public CheckpointList(CheckpointSourceList source) {
            this.source = source;
            this.checkpoints = Lists.newArrayListWithCapacity(source.checkpointSourcesList.size());
            for (CheckpointSource checkpointSource : source.checkpointSourcesList) {
                checkpoints.add(checkpointSource.newCheckpoint());
            }
        }

        private void checkpointComplete(boolean compact) throws IOException {
            for (int i = 0; i < source.checkpointSourcesList.size(); i++) {
                source.checkpointSourcesList.get(i).checkpointComplete(checkpoints.get(i), compact);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(source, checkpoints);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof CheckpointList)) {
                return false;
            }
            Checkpoint other = (Checkpoint) o;
            return 0 == compareTo(other);
        }

        @Override
        public int compareTo(Checkpoint o) {
            if (o == Checkpoint.MAX) {
                return -1;
            } else if (o == Checkpoint.MIN) {
                return 1;
            }

            checkArgument(o instanceof CheckpointList);
            CheckpointList other = (CheckpointList) o;
            if (checkpoints.size() != other.checkpoints.size()) {
                return Integer.compare(checkpoints.size(), other.checkpoints.size());
            }

            for (int i = 0; i < checkpoints.size(); i++) {
                int res = checkpoints.get(i).compareTo(other.checkpoints.get(i));
                if (res != 0) {
                    return res;
                }
            }

            return 0;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(CheckpointList.class)
                .add("checkpoints", checkpoints)
                .toString();
        }

    }

}
