/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.util.MathUtils;

/**
 * A class to hold server statistics.
 */
public class ServerStats {
    private static ServerStats instance = new ServerStats();
    private long packetsSent;
    private long packetsReceived;
    private long maxLatency;
    private long minLatency = Long.MAX_VALUE;
    private long totalLatency = 0;
    private long count = 0;

    public static ServerStats getInstance() {
        return instance;
    }

    protected ServerStats() {
    }

    // getters
    public synchronized long getMinLatency() {
        return (minLatency == Long.MAX_VALUE) ? 0 : minLatency;
    }

    public synchronized long getAvgLatency() {
        if (count != 0) {
            return totalLatency / count;
        }
        return 0;
    }

    public synchronized long getMaxLatency() {
        return maxLatency;
    }

    public synchronized long getPacketsReceived() {
        return packetsReceived;
    }

    public synchronized long getPacketsSent() {
        return packetsSent;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Latency min/avg/max: " + getMinLatency() + "/" + getAvgLatency() + "/" + getMaxLatency() + "\n");
        sb.append("Received: " + getPacketsReceived() + "\n");
        sb.append("Sent: " + getPacketsSent() + "\n");
        return sb.toString();
    }

    synchronized void updateLatency(long requestCreateTime) {
        long latency = MathUtils.now() - requestCreateTime;
        totalLatency += latency;
        count++;
        if (latency < minLatency) {
            minLatency = latency;
        }
        if (latency > maxLatency) {
            maxLatency = latency;
        }
    }

    public synchronized void resetLatency() {
        totalLatency = count = maxLatency = 0;
        minLatency = Long.MAX_VALUE;
    }

    public synchronized void resetMaxLatency() {
        maxLatency = getMinLatency();
    }

    public synchronized void incrementPacketsReceived() {
        packetsReceived++;
    }

    public synchronized void incrementPacketsSent() {
        packetsSent++;
    }

    public synchronized void resetRequestCounters() {
        packetsReceived = packetsSent = 0;
    }

}
