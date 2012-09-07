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
package org.apache.hedwig.exceptions;

import java.util.Collection;
import java.util.Iterator;

import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;

@SuppressWarnings("serial")
public abstract class PubSubException extends Exception {
    protected StatusCode code;

    protected PubSubException(StatusCode code, String msg) {
        super(msg);
        this.code = code;
    }

    protected PubSubException(StatusCode code, Throwable t) {
        super(t);
        this.code = code;
    }

    protected PubSubException(StatusCode code, String msg, Throwable t) {
        super(msg, t);
        this.code = code;
    }

    public static PubSubException create(StatusCode code, String msg) {
        if (code == StatusCode.CLIENT_ALREADY_SUBSCRIBED) {
            return new ClientAlreadySubscribedException(msg);
        } else if (code == StatusCode.CLIENT_NOT_SUBSCRIBED) {
            return new ClientNotSubscribedException(msg);
        } else if (code == StatusCode.MALFORMED_REQUEST) {
            return new MalformedRequestException(msg);
        } else if (code == StatusCode.NO_SUCH_TOPIC) {
            return new NoSuchTopicException(msg);
        } else if (code == StatusCode.NOT_RESPONSIBLE_FOR_TOPIC) {
            return new ServerNotResponsibleForTopicException(msg);
        } else if (code == StatusCode.SERVICE_DOWN) {
            return new ServiceDownException(msg);
        } else if (code == StatusCode.COULD_NOT_CONNECT) {
            return new CouldNotConnectException(msg);
        } else if (code == StatusCode.TOPIC_BUSY) {
            return new TopicBusyException(msg);
        } else if (code == StatusCode.BAD_VERSION) {
            return new BadVersionException(msg);
        } else if (code == StatusCode.NO_TOPIC_PERSISTENCE_INFO) {
            return new NoTopicPersistenceInfoException(msg);
        } else if (code == StatusCode.TOPIC_PERSISTENCE_INFO_EXISTS) {
            return new TopicPersistenceInfoExistsException(msg);
        } else if (code == StatusCode.NO_SUBSCRIPTION_STATE) {
            return new NoSubscriptionStateException(msg);
        } else if (code == StatusCode.SUBSCRIPTION_STATE_EXISTS) {
            return new SubscriptionStateExistsException(msg);
        } else if (code == StatusCode.NO_TOPIC_OWNER_INFO) {
            return new NoTopicOwnerInfoException(msg);
        } else if (code == StatusCode.TOPIC_OWNER_INFO_EXISTS) {
            return new TopicOwnerInfoExistsException(msg);
        } else if (code == StatusCode.INVALID_MESSAGE_FILTER) {
            return new InvalidMessageFilterException(msg);
        }
        /*
         * Insert new ones here
         */
        else if (code == StatusCode.UNCERTAIN_STATE) {
            return new UncertainStateException(msg);
        }
        // Finally the catch all exception (for unexpected error conditions)
        else {
            return new UnexpectedConditionException("Unknow status code:" + code.getNumber() + ", msg: " + msg);
        }
    }

    public StatusCode getCode() {
        return code;
    }

    public static class ClientAlreadySubscribedException extends PubSubException {
        public ClientAlreadySubscribedException(String msg) {
            super(StatusCode.CLIENT_ALREADY_SUBSCRIBED, msg);
        }
    }

    public static class ClientNotSubscribedException extends PubSubException {
        public ClientNotSubscribedException(String msg) {
            super(StatusCode.CLIENT_NOT_SUBSCRIBED, msg);
        }
    }

    public static class MalformedRequestException extends PubSubException {
        public MalformedRequestException(String msg) {
            super(StatusCode.MALFORMED_REQUEST, msg);
        }
    }

    public static class NoSuchTopicException extends PubSubException {
        public NoSuchTopicException(String msg) {
            super(StatusCode.NO_SUCH_TOPIC, msg);
        }
    }

    public static class ServerNotResponsibleForTopicException extends PubSubException {
        // Note the exception message serves as the name of the responsible host
        public ServerNotResponsibleForTopicException(String responsibleHost) {
            super(StatusCode.NOT_RESPONSIBLE_FOR_TOPIC, responsibleHost);
        }
    }

    public static class TopicBusyException extends PubSubException {
        public TopicBusyException(String msg) {
            super(StatusCode.TOPIC_BUSY, msg);
        }
    }

    public static class ServiceDownException extends PubSubException {
        public ServiceDownException(String msg) {
            super(StatusCode.SERVICE_DOWN, msg);
        }

        public ServiceDownException(Exception e) {
            super(StatusCode.SERVICE_DOWN, e);
        }
    }

    public static class CouldNotConnectException extends PubSubException {
        public CouldNotConnectException(String msg) {
            super(StatusCode.COULD_NOT_CONNECT, msg);
        }
    }

    public static class BadVersionException extends PubSubException {
        public BadVersionException(String msg) {
            super(StatusCode.BAD_VERSION, msg);
        }
    }

    public static class NoTopicPersistenceInfoException extends PubSubException {
        public NoTopicPersistenceInfoException(String msg) {
            super(StatusCode.NO_TOPIC_PERSISTENCE_INFO, msg);
        }
    }

    public static class TopicPersistenceInfoExistsException extends PubSubException {
        public TopicPersistenceInfoExistsException(String msg) {
            super(StatusCode.TOPIC_PERSISTENCE_INFO_EXISTS, msg);
        }
    }

    public static class NoSubscriptionStateException extends PubSubException {
        public NoSubscriptionStateException(String msg) {
            super(StatusCode.NO_SUBSCRIPTION_STATE, msg);
        }
    }

    public static class SubscriptionStateExistsException extends PubSubException {
        public SubscriptionStateExistsException(String msg) {
            super(StatusCode.SUBSCRIPTION_STATE_EXISTS, msg);
        }
    }

    public static class NoTopicOwnerInfoException extends PubSubException {
        public NoTopicOwnerInfoException(String msg) {
            super(StatusCode.NO_TOPIC_OWNER_INFO, msg);
        }
    }

    public static class TopicOwnerInfoExistsException extends PubSubException {
        public TopicOwnerInfoExistsException(String msg) {
            super(StatusCode.TOPIC_OWNER_INFO_EXISTS, msg);
        }
    }

    public static class InvalidMessageFilterException extends PubSubException {
        public InvalidMessageFilterException(String msg) {
            super(StatusCode.INVALID_MESSAGE_FILTER, msg);
        }

        public InvalidMessageFilterException(String msg, Throwable t) {
            super(StatusCode.INVALID_MESSAGE_FILTER, msg, t);
        }
    }

    public static class UncertainStateException extends PubSubException {
        public UncertainStateException(String msg) {
            super(StatusCode.UNCERTAIN_STATE, msg);
        }
    }

    // The catch all exception (for unexpected error conditions)
    public static class UnexpectedConditionException extends PubSubException {
        public UnexpectedConditionException(String msg) {
            super(StatusCode.UNEXPECTED_CONDITION, msg);
        }
        public UnexpectedConditionException(String msg, Throwable t) {
            super(StatusCode.UNEXPECTED_CONDITION, msg, t);
        }
    }

    // The composite exception (for concurrent operations).
    public static class CompositeException extends PubSubException {
        private final Collection<PubSubException> exceptions;
        public CompositeException(Collection<PubSubException> exceptions) {
            super(StatusCode.COMPOSITE, compositeMessage(exceptions));
            this.exceptions = exceptions;
        }

        public Collection<PubSubException> getExceptions() {
            return exceptions;
        }

        /** Merges the message fields of the given Exceptions into a one line string. */
        private static String compositeMessage(Collection<PubSubException> exceptions) {
            StringBuilder builder = new StringBuilder("Composite exception: [");
            Iterator<PubSubException> iter = exceptions.iterator();
            if (iter.hasNext())
                builder.append(iter.next().getMessage());
            while (iter.hasNext())
                builder.append(" :: ").append(iter.next().getMessage());
            return builder.append("]").toString();
        }
    }

    public static class ClientNotSubscribedRuntimeException extends RuntimeException {
    }

}
