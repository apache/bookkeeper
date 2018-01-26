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
package org.apache.distributedlog.exceptions;

/**
 * Exception Status Code.
 */
public interface StatusCode {

    /* 2xx: action requested by the client was received, understood, accepted and processed successfully. */

    /* standard response for successful requests. */
    int SUCCESS = 200;

    /* 3xx: client must take additional action to complete the request. */

    /* client closed. */
    int CLIENT_CLOSED = 301;
    /* found the stream in a different server, a redirection is required by client. */
    int FOUND = 302;

    /* 4xx: client seems to have erred. */

    /* request is denied for some reason */
    int REQUEST_DENIED = 403;
    /* request record too large */
    int TOO_LARGE_RECORD = 413;

    /* 5xx: server failed to fulfill an apparently valid request. */

    /* Generic error message, given when no more specific message is suitable. */
    int INTERNAL_SERVER_ERROR = 500;
    /* Not implemented */
    int NOT_IMPLEMENTED = 501;
    /* Already Closed Exception */
    int ALREADY_CLOSED = 502;
    /* Service is currently unavailable (because it is overloaded or down for maintenance). */
    int SERVICE_UNAVAILABLE = 503;
    /* Locking exception */
    int LOCKING_EXCEPTION = 504;
    /* ZooKeeper Errors */
    int ZOOKEEPER_ERROR = 505;
    /* Metadata exception */
    int METADATA_EXCEPTION = 506;
    /* BK Transmit Error */
    int BK_TRANSMIT_ERROR = 507;
    /* Flush timeout */
    int FLUSH_TIMEOUT = 508;
    /* Log empty */
    int LOG_EMPTY = 509;
    /* Log not found */
    int LOG_NOT_FOUND = 510;
    /* Truncated Transactions */
    int TRUNCATED_TRANSACTION = 511;
    /* End of Stream */
    int END_OF_STREAM = 512;
    /* Transaction Id Out of Order */
    int TRANSACTION_OUT_OF_ORDER = 513;
    /* Write exception */
    int WRITE_EXCEPTION = 514;
    /* Stream Unavailable */
    int STREAM_UNAVAILABLE = 515;
    /* Write cancelled exception */
    int WRITE_CANCELLED_EXCEPTION = 516;
    /* over-capacity/backpressure */
    int OVER_CAPACITY = 517;

    /** stream exists but is not ready (recovering etc.).
     the difference between NOT_READY and UNAVAILABLE is that UNAVAILABLE
     indicates the stream is no longer owned by the proxy and we should
     redirect. NOT_READY indicates the stream exist at the proxy but isn't
     eady for writes. */
    int STREAM_NOT_READY = 518;
    /* Region Unavailable */
    int REGION_UNAVAILABLE = 519;
    /* Invalid Enveloped Entry */
    int INVALID_ENVELOPED_ENTRY = 520;
    /* Unsupported metadata version */
    int UNSUPPORTED_METADATA_VERSION = 521;
    /* Log Already Exists */
    int LOG_EXISTS = 522;
    /* Checksum failed on the request */
    int CHECKSUM_FAILED = 523;
    /* Overcapacity: too many streams */
    int TOO_MANY_STREAMS = 524;
    /* Log Segment Not Found */
    int LOG_SEGMENT_NOT_FOUND = 525;
    /* End of Log Segment */
    int END_OF_LOG_SEGMENT = 526;
    /* Log Segment Is Truncated */
    int LOG_SEGMENT_IS_TRUNCATED = 527;

    /* 6xx: unexpected */

    int UNEXPECTED = 600;
    int INTERRUPTED = 601;
    int INVALID_STREAM_NAME = 602;
    int ILLEGAL_STATE = 603;

    /* 10xx: reader exceptions */

    int RETRYABLE_READ = 1000;
    int LOG_READ_ERROR = 1001;
    /* Read cancelled exception */
    int READ_CANCELLED_EXCEPTION = 1002;

}
