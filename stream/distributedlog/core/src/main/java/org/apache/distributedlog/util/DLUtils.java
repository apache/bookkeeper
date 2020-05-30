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
package org.apache.distributedlog.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Objects;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.distributedlog.DistributedLogConstants;
import org.apache.distributedlog.LogSegmentMetadata;
import org.apache.distributedlog.exceptions.InvalidStreamNameException;
import org.apache.distributedlog.exceptions.UnexpectedException;

/**
 * Utilities about DL implementations like uri, log segments, metadata serialization and deserialization.
 */
public class DLUtils {

    /**
     * Find the log segment whose transaction ids are not less than provided <code>transactionId</code>.
     *
     * @param segments
     *          segments to search
     * @param transactionId
     *          transaction id to find
     * @return the first log segment whose transaction ids are not less than <code>transactionId</code>.
     */
    public static int findLogSegmentNotLessThanTxnId(List<LogSegmentMetadata> segments,
                                                     long transactionId) {
        int found = -1;
        for (int i = segments.size() - 1; i >= 0; i--) {
            LogSegmentMetadata segment = segments.get(i);
            if (segment.getFirstTxId() <= transactionId) {
                found = i;
                break;
            }
        }
        if (found <= -1) {
            return -1;
        }
        if (found == 0 && segments.get(0).getFirstTxId() == transactionId) {
            return 0;
        }
        LogSegmentMetadata foundSegment = segments.get(found);
        if (foundSegment.getFirstTxId() == transactionId) {
            for (int i = found - 1; i >= 0; i--) {
                LogSegmentMetadata segment = segments.get(i);
                if (segment.isInProgress()) {
                    break;
                }
                if (segment.getLastTxId() < transactionId) {
                    break;
                }
                found = i;
            }
            return found;
        } else {
            if (foundSegment.isInProgress()
                    || found == segments.size() - 1) {
                return found;
            }
            if (foundSegment.getLastTxId() >= transactionId) {
                return found;
            }
            return found + 1;
        }
    }

    /**
     * Assign next log segment sequence number based on a decreasing list of log segments.
     *
     * @param segmentListDesc
     *          a decreasing list of log segments
     * @return null if no log segments was assigned a sequence number in <code>segmentListDesc</code>.
     *         otherwise, return next log segment sequence number
     */
    public static Long nextLogSegmentSequenceNumber(List<LogSegmentMetadata> segmentListDesc) {
        int lastAssignedLogSegmentIdx = -1;
        Long lastAssignedLogSegmentSeqNo = null;
        Long nextLogSegmentSeqNo = null;

        for (int i = 0; i < segmentListDesc.size(); i++) {
            LogSegmentMetadata metadata = segmentListDesc.get(i);
            if (LogSegmentMetadata.supportsLogSegmentSequenceNo(metadata.getVersion())) {
                lastAssignedLogSegmentSeqNo = metadata.getLogSegmentSequenceNumber();
                lastAssignedLogSegmentIdx = i;
                break;
            }
        }

        if (null != lastAssignedLogSegmentSeqNo) {
            // latest log segment is assigned with a sequence number, start with next sequence number
            nextLogSegmentSeqNo = lastAssignedLogSegmentSeqNo + lastAssignedLogSegmentIdx + 1;
        }
        return nextLogSegmentSeqNo;
    }

    /**
     * Compute the start sequence id for <code>segment</code>, based on previous segment list
     * <code>segmentListDesc</code>.
     *
     * @param logSegmentDescList
     *          list of segments in descending order
     * @param segment
     *          segment to compute start sequence id for
     * @return start sequence id
     */
    public static long computeStartSequenceId(List<LogSegmentMetadata> logSegmentDescList,
                                              LogSegmentMetadata segment)
            throws UnexpectedException {
        long startSequenceId = 0L;
        for (LogSegmentMetadata metadata : logSegmentDescList) {
            if (metadata.getLogSegmentSequenceNumber() >= segment.getLogSegmentSequenceNumber()) {
                continue;
            } else if (metadata.getLogSegmentSequenceNumber() < (segment.getLogSegmentSequenceNumber() - 1)) {
                break;
            }
            if (metadata.isInProgress()) {
                throw new UnexpectedException("Should not complete log segment " + segment.getLogSegmentSequenceNumber()
                        + " since it's previous log segment is still inprogress : " + logSegmentDescList);
            }
            if (metadata.supportsSequenceId()) {
                startSequenceId = metadata.getStartSequenceId() + metadata.getRecordCount();
            }
        }
        return startSequenceId;
    }

    /**
     * Deserialize log segment sequence number for bytes <code>data</code>.
     *
     * @param data
     *          byte representation of log segment sequence number
     * @return log segment sequence number
     * @throws NumberFormatException if the bytes aren't valid
     */
    public static long deserializeLogSegmentSequenceNumber(byte[] data) {
        String seqNoStr = new String(data, UTF_8);
        return Long.parseLong(seqNoStr);
    }

    /**
     * Serilize log segment sequence number <code>logSegmentSeqNo</code> into bytes.
     *
     * @param logSegmentSeqNo
     *          log segment sequence number
     * @return byte representation of log segment sequence number
     */
    public static byte[] serializeLogSegmentSequenceNumber(long logSegmentSeqNo) {
        return Long.toString(logSegmentSeqNo).getBytes(UTF_8);
    }

    /**
     * Deserialize log record transaction id for bytes <code>data</code>.
     *
     * @param data
     *          byte representation of log record transaction id
     * @return log record transaction id
     * @throws NumberFormatException if the bytes aren't valid
     */
    public static long deserializeTransactionId(byte[] data) {
        String seqNoStr = new String(data, UTF_8);
        return Long.parseLong(seqNoStr);
    }

    /**
     * Serilize log record transaction id <code>transactionId</code> into bytes.
     *
     * @param transactionId
     *          log record transaction id
     * @return byte representation of log record transaction id.
     */
    public static byte[] serializeTransactionId(long transactionId) {
        return Long.toString(transactionId).getBytes(UTF_8);
    }

    /**
     * Serialize log segment id into bytes.
     *
     * @param logSegmentId
     *          log segment id
     * @return bytes representation of log segment id
     */
    public static byte[] logSegmentId2Bytes(long logSegmentId) {
        return Long.toString(logSegmentId).getBytes(UTF_8);
    }

    /**
     * Deserialize bytes into log segment id.
     *
     * @param data
     *          bytes representation of log segment id
     * @return log segment id
     */
    public static long bytes2LogSegmentId(byte[] data) {
        return Long.parseLong(new String(data, UTF_8));
    }

    /**
     * Normalize the uri.
     *
     * @param uri the distributedlog uri.
     * @return the normalized uri
     */
    public static URI normalizeURI(URI uri) {
        ServiceURI serviceURI = ServiceURI.create(uri);
        checkNotNull(serviceURI.getServiceName(), "Invalid distributedlog uri : " + uri);
        checkArgument(Objects.equal(DistributedLogConstants.SCHEME_PREFIX, serviceURI.getServiceName()),
                "Unknown distributedlog scheme found : " + uri);
        URI normalizedUri;
        try {
            normalizedUri = new URI(
                    serviceURI.getServiceName(),     // remove backend info
                    uri.getAuthority(),
                    uri.getPath(),
                    uri.getQuery(),
                    uri.getFragment());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid distributedlog uri found : " + uri, e);
        }
        return normalizedUri;
    }

    private static String getHostIpLockClientId() {
        try {
            return InetAddress.getLocalHost().toString();
        } catch (Exception ex) {
            return DistributedLogConstants.UNKNOWN_CLIENT_ID;
        }
    }

    /**
     * Normalize the client id.
     *
     * @return the normalized client id.
     */
    public static String normalizeClientId(String clientId) {
        String normalizedClientId;
        if (clientId.equals(DistributedLogConstants.UNKNOWN_CLIENT_ID)) {
            normalizedClientId = getHostIpLockClientId();
        } else {
            normalizedClientId = clientId;
        }
        return normalizedClientId;
    }

    /**
     * Is it a reserved stream name in bkdl namespace?
     *
     * @param name
     *          stream name
     * @return true if it is reserved name, otherwise false.
     */
    public static boolean isReservedStreamName(String name) {
        return name.startsWith(".") || name.startsWith("<");
    }

    /**
     * Validate the log name.
     *
     * @param logName
     *          name of log
     * @throws InvalidStreamNameException
     */
    public static String validateAndNormalizeName(String logName)
            throws InvalidStreamNameException {
        if (isReservedStreamName(logName)) {
            throw new InvalidStreamNameException(logName, "Log Name is reserved");
        }

        if (logName.charAt(0) == '/') {
            validatePathName(logName);
            return logName.substring(1);
        } else {
            validatePathName("/" + logName);
            return logName;
        }
    }

    private static void validatePathName(String logName) throws InvalidStreamNameException {
        if (logName == null) {
            throw new InvalidStreamNameException("Log name cannot be null");
        } else if (logName.length() == 0) {
            throw new InvalidStreamNameException("Log name length must be > 0");
        } else if (logName.charAt(0) != '/') {
            throw new InvalidStreamNameException("Log name must start with / character");
        } else if (logName.length() != 1) {
            if (logName.charAt(logName.length() - 1) == '/') {
                throw new InvalidStreamNameException("Log name must not end with / character");
            } else {
                String reason = null;
                char lastc = '/';
                char[] chars = logName.toCharArray();

                for (int i = 1; i < chars.length; ++i) {
                    char c = chars[i];
                    if (c == 0) {
                        reason = "null character not allowed @" + i;
                        break;
                    }

                    if (c == '<' || c == '>') {
                        reason = "< or > specified @" + i;
                        break;
                    }

                    if (c == ' ') {
                        reason = "empty space specified @" + i;
                        break;
                    }

                    if (c == '/' && lastc == '/') {
                        reason = "empty node name specified @" + i;
                        break;
                    }

                    if (c == '.' && lastc == '.') {
                        if (chars[i - 2] == '/' && (i + 1 == chars.length || chars[i + 1] == '/')) {
                            reason = "relative paths not allowed @" + i;
                            break;
                        }
                    } else if (c == '.') {
                        if (chars[i - 1] == '/' && (i + 1 == chars.length || chars[i + 1] == '/')) {
                            reason = "relative paths not allowed @" + i;
                            break;
                        }
                    } else if (c > '\u0000' && c < '\u001f'
                        || c > '\u007f' && c < '\u009F'
                        || c > '\ud800' && c < '\uf8ff'
                        || c > '\ufff0' && c < '\uffff') {
                        reason = "invalid character @" + i;
                        break;
                    }
                    lastc = chars[i];
                }

                if (reason != null) {
                    throw new InvalidStreamNameException("Invalid log name \"" + logName + "\" caused by " + reason);
                }
            }
        }

    }
}
