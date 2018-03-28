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

import java.io.IOException;

/**
 * Interface to communicate checkpoint progress.
 */
public interface CheckpointSource {

    /**
     * A checkpoint presented a time point. All entries added before this checkpoint are already persisted.
     */
    interface Checkpoint extends Comparable<Checkpoint> {

        Checkpoint MAX = new Checkpoint() {

            @Override
            public int compareTo(Checkpoint o) {
                if (o == MAX) {
                    return 0;
                }
                return 1;
            }

            @Override
            public boolean equals(Object o) {
                return this == o;
            }

            @Override
            public String toString() {
                return "MAX";
            }

        };

        Checkpoint MIN = new Checkpoint() {
            @Override
            public int compareTo(Checkpoint o) {
                if (o == MIN) {
                    return 0;
                }
                return -1;
            }

            @Override
            public boolean equals(Object o) {
                return this == o;
            }

            @Override
            public String toString() {
                return "MIN";
            }
        };
    }

    /**
     * Request a new a checkpoint.
     *
     * @return checkpoint.
     */
    Checkpoint newCheckpoint();

    /**
     * Tell checkpoint source that the checkpoint is completed.
     * If <code>compact</code> is true, the implementation could compact
     * to reduce size of data containing old checkpoints.
     *
     * @param checkpoint
     *          The checkpoint that has been completed
     * @param compact
     *          Flag to compact old checkpoints.
     */
    void checkpointComplete(Checkpoint checkpoint, boolean compact) throws IOException;
}
