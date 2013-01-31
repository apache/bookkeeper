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
package org.apache.hedwig.jms.message;


import com.google.protobuf.ByteString;
import org.apache.hedwig.jms.SessionImpl;
import org.apache.hedwig.jms.message.header.MetadataProcessor;
import org.apache.hedwig.protocol.PubSubProtocol;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Bunch of simple util methods to reduce code in the implementation.
 */
public class MessageUtil {

    // The various message types supported.
    public enum SupportedMessageTypes {
      ONLY_MESSAGE((byte) 0),
      TEXT((byte) 1),
      BYTES((byte) 2),
      MAP((byte) 3),
      STREAM((byte) 4),
      OBJECT((byte) 5);

      private final byte type;
      private SupportedMessageTypes(byte type){
        this.type = type;
      }

      public byte getType() {
        return type;
      }
    }

    private static final Map<Byte, SupportedMessageTypes> valueToSupportedMessageType;
    static {
        SupportedMessageTypes[] arr = SupportedMessageTypes.values();
        Map<Byte, SupportedMessageTypes> map = new HashMap<Byte, SupportedMessageTypes>(arr.length);
        for (SupportedMessageTypes type : arr){
            map.put(type.getType(), type);
        }
        valueToSupportedMessageType = Collections.unmodifiableMap(map);
    }

    public static boolean asBoolean(Object value) throws MessageFormatException {
        // The JMS spec explicitly wants us to raise NPE !
        // if (null == value) return false;
        if (null == value) return Boolean.valueOf((String) value);

        if (value instanceof Boolean) return (Boolean) value;
        if (value instanceof String) return Boolean.valueOf((String) value);
        throw new MessageFormatException("Unsupported property type " + value.getClass() + " for " + value);
    }

    public static byte asByte(Object value) throws MessageFormatException {
        // The JMS spec explicitly wants us to raise NPE !
        // if (null == value) return 0;
        if (null == value) return Byte.valueOf((String) value);

        if (value instanceof Byte) return (Byte) value;
        if (value instanceof String) return Byte.valueOf((String) value);
        throw new MessageFormatException("Unsupported property type " + value.getClass() + " for " + value);
    }

    public static short asShort(Object value) throws MessageFormatException {
        // The JMS spec explicitly wants us to raise NPE !
        // if (null == value) return 0;
        if (null == value) return Short.valueOf((String) value);

        if (value instanceof Byte) return (Byte) value;
        if (value instanceof Short) return (Short) value;
        if (value instanceof String) return Short.valueOf((String) value);
        throw new MessageFormatException("Unsupported property type " + value.getClass() + " for " + value);
    }

    public static int asInteger(Object value) throws MessageFormatException {
        // The JMS spec explicitly wants us to raise NPE !
        // if (null == value) return 0;
        if (null == value) return Integer.valueOf((String) value);

        if (value instanceof Byte) return (Byte) value;
        if (value instanceof Short) return (Short) value;
        if (value instanceof Integer) return (Integer) value;
        if (value instanceof String) return Integer.valueOf((String) value);
        throw new MessageFormatException("Unsupported property type " + value.getClass() + " for " + value);
    }

    public static long asLong(Object value) throws MessageFormatException {
        // The JMS spec explicitly wants us to raise NPE !
        // if (null == value) return 0;
        if (null == value) return Long.valueOf((String) value);

        if (value instanceof Byte) return (Byte) value;
        if (value instanceof Short) return (Short) value;
        if (value instanceof Integer) return (Integer) value;
        if (value instanceof Long) return (Long) value;
        if (value instanceof String) return Long.valueOf((String) value);
        throw new MessageFormatException("Unsupported property type " + value.getClass() + " for " + value);
    }

    public static float asFloat(Object value) throws MessageFormatException {
        // The JMS spec explicitly wants us to raise NPE !
        // if (null == value) return 0.0f;
        if (null == value) return Float.valueOf((String) value);

        if (value instanceof Float) return (Float) value;
        if (value instanceof String) return Float.valueOf((String) value);
        throw new MessageFormatException("Unsupported property type " + value.getClass() + " for " + value);
    }

    public static double asDouble (Object value) throws MessageFormatException {
        // The JMS spec explicitly wants us to raise NPE !
        // if (null == value) return 0.0;
        if (null == value) return Double.valueOf((String) value);

        if (value instanceof Float) return (Float) value;
        if (value instanceof Double ) return (Double) value;
        if (value instanceof String) return Double.valueOf((String) value);
        throw new MessageFormatException("Unsupported property type " + value.getClass() + " for " + value);
    }

    public static Double asDoubleSelectorProcessing(Object value) throws MessageFormatException {
        if (null == value) return null;

        if (value instanceof Float) return (double) (Float) value;
        if (value instanceof Double ) return (Double) value;

        if (value instanceof Long) return (double) (Long) value;
        if (value instanceof Integer) return (double) (Integer) value;
        if (value instanceof Short) return (double) (Short) value;
        if (value instanceof Byte) return (double) (Byte) value;

        return null;
    }

    public static Integer asIntegerSelectorProcessing(Object value) throws MessageFormatException {
        if (null == value) return null;

        if (value instanceof Float) return (int) (float) (Float) value;
        if (value instanceof Double ) return (int) (double) (Double) value;

        if (value instanceof Long) return (int) (long) (Long) value;
        if (value instanceof Integer) return (Integer) value;
        if (value instanceof Short) return (int) (Short) value;
        if (value instanceof Byte) return (int) (Byte) value;

        return null;
    }

    public static String asString(Object value) {
        if (null == value) return null;

        if (value instanceof String) return (String) value;
        // converts from boolean, byte, short, char, int, long, float and double to String.
        return "" + value;
    }

    public static char asChar(Object value) throws MessageFormatException {
        // treat it as integer with null
        if (null == value) return (char) 0;

        // only from/to char
        if (value instanceof Character) return (Character) value;
        throw new MessageFormatException("Unsupported property type " + value.getClass() + " for " + value);
    }

    public static byte[] asBytes(Object value) throws MessageFormatException {
        if (null == value || value instanceof byte[]) return (byte[]) value;
        throw new MessageFormatException("Unsupported property type " + value.getClass() + " for " + value);
    }

    public static boolean isValidKey(String key) {
        return null != key && 0 != key.length();
    }

    public static byte[] objectToBytes(Object obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        try {
            oos.writeObject(obj);
            oos.flush();
        } finally {
            try { oos.close(); } catch (IOException ioEx) { /* ignore */ }
        }

        return baos.toByteArray();
    }

    public static Object bytesToObject(byte[] data) throws IOException {
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
        try {
            return ois.readObject();
        } catch (ClassNotFoundException  cnfEx){
            // unexpected !
            throw new IllegalStateException("Unexpected", cnfEx);
        } finally {
            try { ois.close(); } catch(IOException ioEx) { /* ignore */ }
        }
    }



    public static MessageImpl processHedwigMessage(SessionImpl session, PubSubProtocol.Message message,
                                                   String sourceTopicName, String subscriberId,
                                                   Runnable ackRunnable) throws JMSException {
        Map<String, Object> map = MetadataProcessor.parseHeaders(message);

        Object jmsBodyTypeValue = map.get(MessageImpl.JMS_MESSAGE_TYPE_KEY);
        // Should we treat these as bytes message by default ?
        // if (! (jmsBodyTypeValue instanceof Byte) )
        //    throw new JMSException("Unsupported message : " + message + ", unable to determine jms message type " +
        //      jmsBodyTypeValue);
        if (! (jmsBodyTypeValue instanceof Byte) ) jmsBodyTypeValue = (Byte) SupportedMessageTypes.BYTES.getType();

        SupportedMessageTypes type = valueToSupportedMessageType.get((Byte) jmsBodyTypeValue);
        switch (type){
            case STREAM:
                return new StreamMessageImpl(session, message, map, sourceTopicName, subscriberId, ackRunnable);
            case MAP:
                return new MapMessageImpl(session, message, map, sourceTopicName, subscriberId, ackRunnable);
            case TEXT:
                return new TextMessageImpl(session, message, map, sourceTopicName, subscriberId, ackRunnable);
            case OBJECT:
                return new ObjectMessageImpl(session, message, map, sourceTopicName, subscriberId, ackRunnable);
            case BYTES:
                return new BytesMessageImpl(session, message, map, sourceTopicName, subscriberId, ackRunnable);
            case ONLY_MESSAGE:
                return new MessageImpl(session, message, map, sourceTopicName, subscriberId, ackRunnable);
            default:
                throw new JMSException("Unsupported message type : " + type + " for message " + message);
        }
    }

    public static MessageImpl createMessageCopy(SessionImpl session, Message message) throws JMSException {
        if (message instanceof MessageImpl) {
            return createMessageImplCopy(session, (MessageImpl) message);
        }

        if (message instanceof BytesMessage) {
            return new BytesMessageImpl((BytesMessage) message, session);
        }
        if (message instanceof MapMessage) {
            return new MapMessageImpl((MapMessage) message, session);
        }
        if (message instanceof ObjectMessage) {
            return new ObjectMessageImpl((ObjectMessage) message, session);
        }
        if (message instanceof StreamMessage) {
            return new StreamMessageImpl((StreamMessage) message, session);
        }
        if (message instanceof TextMessage) {
            return new TextMessageImpl((TextMessage) message, session);
        }

        return new MessageImpl(message, session);
    }

    private static MessageImpl createMessageImplCopy(SessionImpl session, MessageImpl message)
        throws JMSException {

        if (message instanceof BytesMessageImpl) {
            return new BytesMessageImpl(session, (BytesMessageImpl) message, message.getSourceName(),
                message.getSubscriberId());
        }
        if (message instanceof MapMessageImpl) {
            return new MapMessageImpl(session, (MapMessageImpl) message, message.getSourceName(),
                message.getSubscriberId());
        }
        if (message instanceof ObjectMessageImpl) {
            return new ObjectMessageImpl(session, (ObjectMessageImpl) message, message.getSourceName(),
                message.getSubscriberId());
        }
        if (message instanceof StreamMessageImpl) {
            return new StreamMessageImpl(session, (StreamMessageImpl) message, message.getSourceName(),
                message.getSubscriberId());
        }
        if (message instanceof TextMessageImpl) {
            return new TextMessageImpl(session, (TextMessageImpl) message, message.getSourceName(),
                message.getSubscriberId());
        }

        return new MessageImpl(session, message, message.getSourceName(), message.getSubscriberId());
    }

    private static final String JMS_MESSAGE_ID_PREFIX = "ID:";
    private static final String LOCAL_PREFIX = "LOCAL(";
    private static final String REMOTE_PREFIX = "REMOTE(";
    private static final char SEQ_ID_SUFFIX = ')';
    private static final char REMOTE_RECORD_SEPARATOR = ',';
    private static final char REMOTE_RECORD_SEQ_ID_PREFIX = '[';
    private static final char REMOTE_RECORD_SEQ_ID_SUFFIX = ']';
    private static final Pattern remoteMessageIdSplitPattern = Pattern.compile("" + REMOTE_RECORD_SEPARATOR);

    /**
     * Based on
     * {@link org.apache.hedwig.admin.console.ReadTopic#formatMessage(PubSubProtocol.Message)}
     *
     * This is tightly coupled with
     * @see #generateSeqIdFromJMSMessageId(String)
     *
     * @param seqId The sequence id to convert to string.
     * @return The string representation of the seq-id.
     */
    public static String generateJMSMessageIdFromSeqId(final PubSubProtocol.MessageSeqId seqId) {
        StringBuilder sb = new StringBuilder();
        // mandatory prefix for system generated id's.
        sb.append(JMS_MESSAGE_ID_PREFIX);

        if (seqId.hasLocalComponent()) {
            sb.append(LOCAL_PREFIX).append(seqId.getLocalComponent()).append(SEQ_ID_SUFFIX);
        } else {
            List<PubSubProtocol.RegionSpecificSeqId> remoteIds = seqId.getRemoteComponentsList();
            boolean first = true;

            sb.append(REMOTE_PREFIX);
            for (PubSubProtocol.RegionSpecificSeqId rssid : remoteIds) {
                if (!first) sb.append(REMOTE_RECORD_SEPARATOR);
                first = false;
                sb.append(rssid.getRegion().toStringUtf8());
                sb.append(REMOTE_RECORD_SEQ_ID_PREFIX);
                sb.append(rssid.getSeqId());
                sb.append(REMOTE_RECORD_SEQ_ID_SUFFIX);
            }
            sb.append(SEQ_ID_SUFFIX);
        }

        return sb.toString();
    }

    /**
     * Based on
     * {@link org.apache.hedwig.admin.console.ReadTopic#formatMessage(PubSubProtocol.Message)}
     *
     * This is tightly coupled with
     * @see #generateJMSMessageIdFromSeqId(org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId)
     * @param messageId The message id to convert to string.
     * @return The seq-id
     * @throws javax.jms.JMSException In case of exceptions doing the conversion.
     */
    public static PubSubProtocol.MessageSeqId generateSeqIdFromJMSMessageId(final String messageId)
        throws JMSException {
        if (null == messageId || !messageId.startsWith(JMS_MESSAGE_ID_PREFIX)) {
            throw new JMSException("Invalid messageId specified '" + messageId + "'");
        }

        PubSubProtocol.MessageSeqId.Builder builder = PubSubProtocol.MessageSeqId.newBuilder();
        // local ?
        if (messageId.regionMatches(JMS_MESSAGE_ID_PREFIX.length(), LOCAL_PREFIX, 0, LOCAL_PREFIX.length())){
            try {
                long seqId = Long.parseLong(messageId.substring(JMS_MESSAGE_ID_PREFIX.length() +
                    LOCAL_PREFIX.length(), messageId.length() - 1));
                builder.setLocalComponent(seqId);
            } catch (NumberFormatException nfEx){
                JMSException jEx = new JMSException("Unable to parse local seq id from '" +
                    messageId + "' .. " + nfEx);
                jEx.setLinkedException(nfEx);
                throw jEx;
            }
        }
        else {
            assert messageId.regionMatches(JMS_MESSAGE_ID_PREFIX.length(), REMOTE_PREFIX, 0,
                REMOTE_PREFIX.length());

            final String[] remoteParts;
            {
                final String remoteMessageId = messageId.substring(JMS_MESSAGE_ID_PREFIX.length() +
                    REMOTE_PREFIX.length(), messageId.length() - 1);
                // Should ew stop using pattern and move to using indexOf's ?
                remoteParts = remoteMessageIdSplitPattern.split(remoteMessageId);
            }

            for (String remote : remoteParts){
                if (REMOTE_RECORD_SEQ_ID_SUFFIX != remote.charAt(remote.length() - 1))
                  throw new JMSException("Invalid remote region snippet (no seq suffix) '" +
                      remote + "' within '" + messageId);
                final int regionIndx = remote.indexOf(REMOTE_RECORD_SEQ_ID_PREFIX);
                if (-1 == regionIndx)
                  throw new JMSException("Invalid remote region snippet (no region) '" + remote +
                      "' within '" + messageId);
                final String region = remote.substring(0, regionIndx);
                final long seqId;


                try {
                    seqId = Long.parseLong(remote.substring(regionIndx + 1, remote.length() - 1));
                } catch (NumberFormatException nfEx){
                    JMSException jEx = new JMSException("Unable to parse remote seq id from '" +
                        remote + "' within '" + messageId + "' .. " + nfEx);
                    jEx.setLinkedException(nfEx);
                    throw jEx;
                }

                PubSubProtocol.RegionSpecificSeqId.Builder rbuilder =
                    PubSubProtocol.RegionSpecificSeqId.newBuilder();
                rbuilder.setRegion(ByteString.copyFromUtf8(region));
                rbuilder.setSeqId(seqId);
                builder.addRemoteComponents(rbuilder);
            }
        }

        return builder.build();
    }

    public static MessageImpl createCloneForDispatch(SessionImpl session, MessageImpl msg,
                                                     String sourceTopicName, String subscriberId) throws JMSException {
        MessageImpl retval = msg.createClone(session, sourceTopicName, subscriberId);
        retval.reset();
        return retval;
    }
}
