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
import org.apache.hedwig.jms.selector.SelectorEvaluationException;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of a message.
 */
public class MessageImpl implements Message {

    // This is of type byte for now - enough ?
    public static final String JMS_MESSAGE_TYPE_KEY = "jmsBodyType";
    // 'others' (non-jms clients) can depend on this boolean metadata property : for now, part
    // of jms values directly due to how metadata is being designed !
    // sigh :-(
    public static final String EMPTY_BODY_KEY = "bodyEmpty";


    private final static Logger logger = LoggerFactory.getLogger(MessageImpl.class);

    public static final String JMS_MESSAGE_ID = "JMSMessageID";
    public static final String JMS_TIMESTAMP = "JMSTimestamp";
    public static final String JMS_CORRELATION_ID = "JMSCorrelationID";
    public static final String JMS_REPLY_TO = "JMSReplyTo";
    public static final String JMS_DESTINATION = "JMSDestination";
    public static final String JMS_DELIVERY_MODE = "JMSDeliveryMode";
    public static final String JMS_REDELIVERED = "JMSRedelivered";
    public static final String JMS_TYPE = "JMSType";
    public static final String JMS_EXPIRATION = "JMSExpiration";
    public static final String JMS_PRIORITY = "JMSPriority";

    private static final Set<String> standardProperties;
    static {
        Set<String> set = new HashSet<String>(16);
        set.add(JMS_MESSAGE_ID);
        set.add(JMS_TIMESTAMP);
        set.add(JMS_CORRELATION_ID);

        set.add(JMS_REPLY_TO);
        set.add(JMS_DESTINATION);
        set.add(JMS_DELIVERY_MODE);

        // Currently simulated in provider - NOT from hedwig.
        set.add(JMS_REDELIVERED);
        set.add(JMS_TYPE);

        set.add(JMS_EXPIRATION);
        set.add(JMS_PRIORITY);

        standardProperties = Collections.unmodifiableSet(set);
    }

    private final SessionImpl session;
    private final String serverJmsMessageId;

    private String jmsMessageId;
    private long jmsTimestamp = 0;
    private String jmsCorrelationID;

    private Destination jmsReplyTo;
    private Destination jmsDestination;
    private int jmsDeliveryMode = DeliveryMode.PERSISTENT;

    private boolean jmsRedelivered;
    private String jmsType;

    private long jmsExpiration = 0L;
    private int jmsPriority = Message.DEFAULT_PRIORITY;

    // Note: this DOES NOT contain standard headers - which are explicitly handled.
    private boolean propertiesReadOnly = false;
    protected Map<String, Object> properties = new HashMap<String, Object>(4);

    // key == standard property.
    private Set<String> standardPropertiesExists = new HashSet<String>(16);
    private Set<String> standardPropertiesExistsForWire = new HashSet<String>(16);

    private final String sourceName;
    private final String subscriberId;

    private final Runnable ackRunnable;

    // This is to be set to true ONLY for testing - NOT otherwise !
    // The JMS api DOES NOT expose this ...
    // private boolean allowSpecifyJMSMessageIDForTest;

    //private final PubSubProtocol.Message rawMessage;

    public MessageImpl(SessionImpl session){
        this.session = session;

        this.sourceName = null;
        this.subscriberId = null;
        this.ackRunnable = null;
        this.serverJmsMessageId = null;
        // this.rawMessage = null;
    }

    MessageImpl(SessionImpl session, MessageImpl message, String sourceName, String subscriberId)
        throws JMSException {
        this.session = session;
        this.sourceName = sourceName;
        this.subscriberId = subscriberId;
        this.ackRunnable = message.getAckRunnable();
        this.serverJmsMessageId = message.getServerJmsMessageId();
        // this.rawMessage = null;

        // Copy all properties from message to this class.

        this.properties.putAll(message.properties);

        // Now copy rest of the state over ...
        if (message.propertyExists(JMS_MESSAGE_ID)) setJMSMessageIDInternal(message.getJMSMessageID());
        if (message.propertyExists(JMS_TIMESTAMP)) setJMSTimestamp(message.getJMSTimestamp());
        if (message.propertyExists(JMS_CORRELATION_ID)) setJMSCorrelationID(message.getJMSCorrelationID());
        // We do not support this right now.
        // if (message.propertyExists(JMS_CORRELATION_ID_AS_BYTES))
        //    setJMSCorrelationIDAsBytes(message.getJMSCorrelationIDAsBytes());
        if (message.propertyExists(JMS_REPLY_TO)) setJMSReplyTo(message.getJMSReplyTo());
        if (message.propertyExists(JMS_DESTINATION)) setJMSDestination(message.getJMSDestination());
        if (message.propertyExists(JMS_DELIVERY_MODE)) setJMSDeliveryMode(message.getJMSDeliveryMode());
        if (message.propertyExists(JMS_REDELIVERED)) setJMSRedelivered(message.getJMSRedelivered());
        if (message.propertyExists(JMS_TYPE)) setJMSType(message.getJMSType());
        if (message.propertyExists(JMS_EXPIRATION)) setJMSExpiration(message.getJMSExpiration());
        if (message.propertyExists(JMS_PRIORITY)) setJMSPriority(message.getJMSPriority());

        this.propertiesReadOnly = message.propertiesReadOnly;
    }

    // To clone a message from a Message which is NOT MessageImpl
    // Changing order of parameter to NOT accidentally clash with the constructor above.
    // This is midly confusing, but helps a lot in preventing accidental bugs !
    MessageImpl(Message message, SessionImpl session) throws JMSException {
        this.session = session;
        this.sourceName = null;
        this.subscriberId = null;
        this.ackRunnable = null;
        this.serverJmsMessageId = null;
        // this.rawMessage = null;

        assert (! (message instanceof MessageImpl ));

        // Copy all properties from message to this class.
        Enumeration names = message.getPropertyNames();
        while (names.hasMoreElements()){
            Object name = names.nextElement();
            if (!(name instanceof String))
              throw new JMSException("Unsupported type (expected String) for key : " + name);

            String sname = (String) name;
            this.properties.put(sname, message.getObjectProperty(sname));
        }

        // Now copy rest of the state over ...
        // JMS VIOLATION: we will be unable to check for propertyExists after this,
        //  at sender and receiver side ... sigh :-(
        setJMSMessageIDInternal(message.getJMSMessageID());
        setJMSTimestamp(message.getJMSTimestamp());
        setJMSCorrelationID(message.getJMSCorrelationID());
        // We do not support this right now.
        // setJMSCorrelationIDAsBytes(message.getJMSCorrelationIDAsBytes());
        setJMSReplyTo(message.getJMSReplyTo());
        setJMSDestination(message.getJMSDestination());
        setJMSDeliveryMode(message.getJMSDeliveryMode());
        setJMSRedelivered(message.getJMSRedelivered());
        setJMSType(message.getJMSType());
        setJMSExpiration(message.getJMSExpiration());
        setJMSPriority(message.getJMSPriority());

        // Should be able to modify, right ?
        this.propertiesReadOnly = false;

        // remove all jms standard keys from properties now : this should ideally result in zero
        // removals ... but we never know with client code !
        for (String key : standardProperties) properties.remove(key);
    }

    MessageImpl(SessionImpl session, PubSubProtocol.Message message, Map<String, Object> properties,
                String sourceName, String subscriberId, Runnable ackRunnable) throws JMSException {
        this.session = session;
        this.sourceName = sourceName;
        this.subscriberId = subscriberId;
        this.ackRunnable = ackRunnable;
        // this.rawMessage = message;

        // setJMSMessageID(getStringProperty(properties, JMS_MESSAGE_ID));
        setJMSMessageIDInternal(MessageUtil.generateJMSMessageIdFromSeqId(message.getMsgId()));
        this.serverJmsMessageId = getJMSMessageID();

        if (properties.containsKey(JMS_TIMESTAMP)) setJMSTimestamp(getLongProperty(properties, JMS_TIMESTAMP));
        if (properties.containsKey(JMS_CORRELATION_ID)) setJMSCorrelationID(
            getStringProperty(properties, JMS_CORRELATION_ID));
        if (null != getStringProperty(properties, JMS_REPLY_TO)) {
            setJMSReplyTo(
                    session.getDestination(session.findDestinationType(getStringProperty(properties, JMS_REPLY_TO)),
                        getStringProperty(properties, JMS_REPLY_TO)
                    ));
        }
        if (null != getStringProperty(properties, JMS_DESTINATION)) {
            setJMSDestination(
                    session.getDestination(session.findDestinationType(
                        getStringProperty(properties, JMS_DESTINATION)),
                        getStringProperty(properties, JMS_DESTINATION)
                    ));
        }

        if (properties.containsKey(JMS_DELIVERY_MODE)) setJMSDeliveryMode(
            getIntProperty(properties, JMS_DELIVERY_MODE));
        if (properties.containsKey(JMS_TYPE)) setJMSType(getStringProperty(properties, JMS_TYPE));

        if (properties.containsKey(JMS_EXPIRATION)) setJMSExpiration(
            getLongProperty(properties, JMS_EXPIRATION));
        if (properties.containsKey(JMS_PRIORITY)) setJMSPriority(
            getIntProperty(properties, JMS_PRIORITY));


        // remove all jms standard keys from properties now : this should result in zero removals ...
        // but adding anyway.
        for (String key : standardProperties) properties.remove(key);

        // Immutable after reading from stream !
        this.propertiesReadOnly = true;
        this.properties.putAll(properties);
    }

    protected MessageUtil.SupportedMessageTypes getJmsMessageType(){
        // Validate against coding bug ... this MUST be overridden in all subclasses.
        if (getClass() != MessageImpl.class)
          throw new IllegalStateException("This method must be overridden by subclasses. class : " + getClass());
        return MessageUtil.SupportedMessageTypes.ONLY_MESSAGE;
    }

    public PubSubProtocol.Message generateHedwigMessage() throws JMSException {
        // This is to be called ONLY from the base class - all children MUST override it and NOT delegate to it.
        if (getClass() != MessageImpl.class) {
            throw new JMSException("Unexpected to call MessageImpl's generateHedwigMessage from subclass " +
                getClass());
        }

        PubSubProtocol.Message.Builder builder = PubSubProtocol.Message.newBuilder();
        populateBuilderWithHeaders(builder);
        // no body - will be appropriately set in populateBuilderWithHeaders().
        return builder.build();
    }

    protected boolean isBodyEmpty(){
        return true;
    }

    /*
    protected void markEmptyBody(PubSubProtocol.Message.Builder builder) {
        MetadataProcessor.addBooleanProperty(builder, EMPTY_BODY_KEY, true);
        builder.setBody(ByteString.EMPTY);
    }
    */

    protected boolean hasBodyFromProperties() {
        // if key missing (common case), then there is body.
        if (!properties.containsKey(EMPTY_BODY_KEY)) return true;
        // If present, then check if it is a boolean of value true.
        Object value = properties.get(EMPTY_BODY_KEY);

        // special case null.
        if (null == value) return true;
        if (value instanceof Boolean) return ! (Boolean) value;

        // unknown type ...
        logger.info("Unknown type for value of " + EMPTY_BODY_KEY + " in message properties : " + value);
        // assume true by default.
        return true;
    }


    protected final void populateBuilderWithHeaders(PubSubProtocol.Message.Builder builder) throws JMSException {

        Map<String, Object> propertiesCopy = new HashMap<String, Object>(properties);
        if (isBodyEmpty()) {
            propertiesCopy.put(EMPTY_BODY_KEY, true);
            builder.setBody(ByteString.EMPTY);
        }
        // Not setting unless required to reduce message size - change this ?
        // else propertiesCopy.put(EMPTY_BODY_KEY, false);

        Iterator<Map.Entry<String, Object>> iter = propertiesCopy.entrySet().iterator();
        while (iter.hasNext()){
            Map.Entry<String, Object> entry = iter.next();
            if (standardProperties.contains(entry.getKey())) {
                if (logger.isInfoEnabled())
                  logger.info("Ignoring user attempt to set standard property as application property : " + entry);
                iter.remove();
            }
        }


        // set jms message type.
        propertiesCopy.put(JMS_MESSAGE_TYPE_KEY, getJmsMessageType().getType());
        if (standardPropertiesExistsForWire.contains(JMS_CORRELATION_ID))
          propertiesCopy.put(JMS_CORRELATION_ID, getJMSCorrelationID());

        // unsupported for now.
        // if (standardPropertiesExistsForWire.contains(JMS_CORRELATION_ID_AS_BYTES))
        //    propertiesCopy.put(JMS_CORRELATION_ID_AS_BYTES, getJMSCorrelationIDAsBytes());
        if (standardPropertiesExistsForWire.contains(JMS_DELIVERY_MODE))
          propertiesCopy.put(JMS_DELIVERY_MODE, getJMSDeliveryMode());

        if (standardPropertiesExistsForWire.contains(JMS_DESTINATION))
          propertiesCopy.put(JMS_DESTINATION, session.toName(getJMSDestination()));
        if (standardPropertiesExistsForWire.contains(JMS_EXPIRATION))
          propertiesCopy.put(JMS_EXPIRATION, getJMSExpiration());

        // This can be set by client - but we ignore it in hedwig.
        // if (standardPropertiesExistsForWire.contains(JMS_MESSAGE_ID))
        //    propertiesCopy.put(JMS_MESSAGE_ID, getJMSMessageID());

        // We do not support priority - but we are gong to allow it to be specified : this is
        // for selectors to set conditions on it !
        if (standardPropertiesExistsForWire.contains(JMS_PRIORITY))
          propertiesCopy.put(JMS_PRIORITY, getJMSPriority());

        // this is not to be sent to hedwig.
        // if (standardPropertiesExistsForWire.contains(JMS_REDELIVERED))
        //    propertiesCopy.put(JMS_REDELIVERED, getJMSRedelivered());

        if (standardPropertiesExistsForWire.contains(JMS_REPLY_TO))
          propertiesCopy.put(JMS_REPLY_TO, session.toName(getJMSReplyTo()));


        propertiesCopy.put(JMS_TIMESTAMP, getJMSTimestamp());
        if (standardPropertiesExistsForWire.contains(JMS_TYPE)) propertiesCopy.put(JMS_TYPE, getJMSType());


        MetadataProcessor.addHeaders(builder, propertiesCopy);
    }

    @Override
    public String getJMSMessageID() {
        return jmsMessageId;
    }

    @Override
    public void setJMSMessageID(String jmsMessageId) throws JMSException {
        // JMS VIOLATION ... we are NOT allowing client to override jms message-id.
        // if (!allowSpecifyJMSMessageIDForTest)
        //    throw new JMSException("We do not allow setting jms message id. This will be ignored by hedwig anyway.");
        if (logger.isDebugEnabled()) logger.debug("Setting this is irrelevant - we override it anyway - " +
            " hedwig does not allow specifying it explictly.");
        setJMSMessageIDInternal(jmsMessageId);
    }

    public void setJMSMessageIDInternal(String jmsMessageId) throws JMSException {
        this.jmsMessageId = jmsMessageId;
        if (null != jmsMessageId){
            // We do not allow sending the property over wire.
            this.standardPropertiesExists.add(JMS_MESSAGE_ID);
            // this.standardPropertiesExistsForWire.add(JMS_MESSAGE_ID);
        }
        else {
            this.standardPropertiesExists.remove(JMS_MESSAGE_ID);
            // this.standardPropertiesExistsForWire.remove(JMS_MESSAGE_ID);
        }
    }

    // The immutable message Id set by the server.
    public String getServerJmsMessageId() {
        return serverJmsMessageId;
    }

    @Override
    public long getJMSTimestamp() {
        return jmsTimestamp;
    }

    @Override
    public void setJMSTimestamp(long jmsTimestamp) {
        this.jmsTimestamp = jmsTimestamp;
        this.standardPropertiesExists.add(JMS_TIMESTAMP);
        // this.standardPropertiesExistsForWire.add(JMS_TIMESTAMP);
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() {
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] bytes) {
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public void setJMSCorrelationID(String jmsCorrelationID) {
        this.jmsCorrelationID = jmsCorrelationID;
        if (null != jmsCorrelationID){
            this.standardPropertiesExists.add(JMS_CORRELATION_ID);
            this.standardPropertiesExistsForWire.add(JMS_CORRELATION_ID);
        }
        else {
            this.standardPropertiesExists.remove(JMS_CORRELATION_ID);
            this.standardPropertiesExistsForWire.remove(JMS_CORRELATION_ID);
        }
    }

    @Override
    public String getJMSCorrelationID() {
        return jmsCorrelationID;
    }

    @Override
    public Destination getJMSReplyTo() {
        return jmsReplyTo;
    }

    @Override
    public void setJMSReplyTo(Destination jmsReplyTo) {
        this.jmsReplyTo = jmsReplyTo;
        if (null != jmsReplyTo){
            this.standardPropertiesExists.add(JMS_REPLY_TO);
            this.standardPropertiesExistsForWire.add(JMS_REPLY_TO);
        }
        else {
            this.standardPropertiesExists.remove(JMS_REPLY_TO);
            this.standardPropertiesExistsForWire.remove(JMS_REPLY_TO);
        }
    }

    @Override
    public Destination getJMSDestination() {
        return jmsDestination;
    }

    @Override
    public void setJMSDestination(Destination jmsDestination) {
        this.jmsDestination = jmsDestination;
        if (null != jmsDestination){
            this.standardPropertiesExists.add(JMS_DESTINATION);
            this.standardPropertiesExistsForWire.add(JMS_DESTINATION);
        }
        else {
            this.standardPropertiesExists.remove(JMS_DESTINATION);
            this.standardPropertiesExistsForWire.remove(JMS_DESTINATION);
        }
    }

    @Override
    public int getJMSDeliveryMode() {
        return jmsDeliveryMode;
    }

    @Override
    public void setJMSDeliveryMode(int jmsDeliveryMode) {
        this.jmsDeliveryMode = jmsDeliveryMode;
        this.standardPropertiesExists.add(JMS_DELIVERY_MODE);
        this.standardPropertiesExistsForWire.add(JMS_DELIVERY_MODE);
    }

    @Override
    public boolean getJMSRedelivered() {
        return jmsRedelivered;
    }

    @Override
    public void setJMSRedelivered(boolean jmsRedelivered) {
        this.jmsRedelivered = jmsRedelivered;
        this.standardPropertiesExists.add(JMS_REDELIVERED);
        // this.standardPropertiesExistsForWire.add(JMS_REDELIVERED);
    }

    @Override
    public String getJMSType() {
        return jmsType;
    }

    @Override
    public void setJMSType(String jmsType) {
        this.jmsType = jmsType;
        if (null != jmsType){
            this.standardPropertiesExists.add(JMS_TYPE);
            this.standardPropertiesExistsForWire.add(JMS_TYPE);
        }
        else {
            this.standardPropertiesExists.remove(JMS_TYPE);
            this.standardPropertiesExistsForWire.remove(JMS_TYPE);
        }
    }

    @Override
    public long getJMSExpiration() {
        return jmsExpiration;
    }

    @Override
    public void setJMSExpiration(long jmsExpiration) {
        // We simulate it now !
        // if (logger.isInfoEnabled()) logger.info("JMSExpiration is not supported right now by Hedwig ...");
        this.jmsExpiration = jmsExpiration;

        if (0 != jmsExpiration){
            this.standardPropertiesExists.add(JMS_EXPIRATION);
            this.standardPropertiesExistsForWire.add(JMS_EXPIRATION);
        }
        else {
            this.standardPropertiesExists.remove(JMS_EXPIRATION);
            this.standardPropertiesExistsForWire.remove(JMS_EXPIRATION);
        }
    }

    @Override
    public int getJMSPriority() {
        return jmsPriority;
    }

    @Override
    public void setJMSPriority(int jmsPriority) {
        this.jmsPriority = jmsPriority;
        this.standardPropertiesExists.add(JMS_PRIORITY);
        // Sent over wire ?
        this.standardPropertiesExistsForWire.add(JMS_PRIORITY);
    }

    @Override
    public void clearProperties() {
        this.propertiesReadOnly = false;
        properties.clear();
    }

    /**
     * JMS VIOLATION ? The spec & javadoc is unclear as to whether this method must include jms
     * standard properties or not.
     * But going by javadoc of
     * @see #getPropertyNames() , we have this specified :
     * "Note that JMS standard header fields are not considered properties and are not returned
     * in this enumeration."
     * Which indicates this method must not include standard properties.
     */
    @Override
    public boolean propertyExists(String key) {
        if (!standardProperties.contains(key)) return properties.containsKey(key);

        // Evaluate depending on type of property.
        return standardPropertiesExists.contains(key);
    }

    @Override
    public boolean getBooleanProperty(String key) throws JMSException {
        checkIfStandardProperty(key);
        return getBooleanProperty(properties, key);
    }

    private boolean getBooleanProperty(Map<String, Object> properties, String key) throws JMSException {
        return MessageUtil.asBoolean(properties.get(key));
    }

    @Override
    public byte getByteProperty(String key) throws JMSException {
        checkIfStandardProperty(key);
        return getByteProperty(properties, key);
    }

    private byte getByteProperty(Map<String, Object> properties, String key) throws JMSException {
        return MessageUtil.asByte(properties.get(key));
    }

    @Override
    public short getShortProperty(String key) throws JMSException {
        checkIfStandardProperty(key);
        return getShortProperty(properties, key);
    }

    private short getShortProperty(Map<String, Object> properties, String key) throws JMSException {
        return MessageUtil.asShort(properties.get(key));
    }

    @Override
    public int getIntProperty(String key) throws JMSException {
        checkIfStandardProperty(key);
        return getIntProperty(properties, key);
    }

    private int getIntProperty(Map<String, Object> properties, String key) throws JMSException {
        return MessageUtil.asInteger(properties.get(key));
    }

    @Override
    public long getLongProperty(String key) throws JMSException {
        checkIfStandardProperty(key);
        return getLongProperty(properties, key);
    }

    private long getLongProperty(Map<String, Object> properties, String key) throws JMSException {
        return MessageUtil.asLong(properties.get(key));
    }

    @Override
    public float getFloatProperty(String key) throws JMSException {
        checkIfStandardProperty(key);
        return getFloatProperty(properties, key);
    }

    private float getFloatProperty(Map<String, Object> properties, String key) throws JMSException {
        return MessageUtil.asFloat(properties.get(key));
    }

    @Override
    public double getDoubleProperty(String key) throws JMSException {
        checkIfStandardProperty(key);
        return getDoubleProperty(properties, key);
    }

    private double getDoubleProperty(Map<String, Object> properties, String key) throws JMSException {
        return MessageUtil.asDouble(properties.get(key));
    }

    public Object getSelectorProcessingPropertyValue(String key) throws SelectorEvaluationException {
        if (properties.containsKey(key)) return properties.get(key);
        if (! standardProperties.contains(key)) return null;

        if (JMS_MESSAGE_ID.equals(key)) return getJMSMessageID();
        if (JMS_TIMESTAMP.equals(key)) return getJMSTimestamp();
        if (JMS_CORRELATION_ID.equals(key)) return getJMSCorrelationID();
        // We do not support this right now.
        // if (JMS_CORRELATION_ID_AS_BYTES.equals(key)) return getJMSCorrelationIDAsBytes();
        if (JMS_REPLY_TO.equals(key)) return getJMSReplyTo();
        if (JMS_DESTINATION.equals(key)) return getJMSDestination();
        if (JMS_DELIVERY_MODE.equals(key)) {
            // 3.8.1.3 Special Notes "When used in a message selector JMSDeliveryMode is treated as having the
            // values ‘PERSISTENT’ and ‘NON_PERSISTENT’."
            final int deliveryMode = getJMSDeliveryMode();
            if (DeliveryMode.PERSISTENT == deliveryMode) return "PERSISTENT";
            if (DeliveryMode.NON_PERSISTENT == deliveryMode) return "NON_PERSISTENT";
            // unknown !
            if (logger.isInfoEnabled()) logger.info("Unknown delivery mode specified ... " + deliveryMode);
            return null;
        }
        if (JMS_REDELIVERED.equals(key)) return getJMSRedelivered();
        if (JMS_TYPE.equals(key)) return getJMSType();
        if (JMS_EXPIRATION.equals(key)) return getJMSExpiration();
        if (JMS_PRIORITY.equals(key)) return getJMSPriority();

        throw new SelectorEvaluationException("Unable to retrieve value for key : '" + key + "'");
    }

    @Override
    public String getStringProperty(String key) throws JMSException {
        checkIfStandardProperty(key);
        return getStringProperty(properties, key);
    }

    private String getStringProperty(Map<String, Object> properties, String key) throws JMSException {
        return MessageUtil.asString(properties.get(key));
    }

    @Override
    public Object getObjectProperty(String key) throws JMSException {
        checkIfStandardProperty(key);
        // if (!propertyExists(key)) return null;

        return properties.get(key);
    }

    @Override
    public Enumeration<String> getPropertyNames() throws JMSException {
        return Collections.enumeration(properties.keySet());
    }

    @Override
    public void setBooleanProperty(String key, boolean value) throws JMSException {
        if (!MessageUtil.isValidKey(key)) throw new IllegalArgumentException("Invalid key " + key);
        if (propertiesReadOnly)
          throw new MessageNotWriteableException("Message not writable. attempt to set property " +
              key + " = " + value);
        checkIfStandardProperty(key);
        properties.put(key, value);
    }

    @Override
    public void setByteProperty(String key, byte value) throws JMSException {
        if (!MessageUtil.isValidKey(key)) throw new IllegalArgumentException("Invalid key " + key);
        if (propertiesReadOnly)
          throw new MessageNotWriteableException("Message not writable. attempt to set property " +
              key + " = " + value);
        checkIfStandardProperty(key);
        properties.put(key, value);
    }

    @Override
    public void setShortProperty(String key, short value) throws JMSException {
        if (!MessageUtil.isValidKey(key)) throw new IllegalArgumentException("Invalid key " + key);
        if (propertiesReadOnly)
          throw new MessageNotWriteableException("Message not writable. attempt to set property " +
              key + " = " + value);
        checkIfStandardProperty(key);
        properties.put(key, value);
    }

    @Override
    public void setIntProperty(String key, int value) throws JMSException {
        if (!MessageUtil.isValidKey(key)) throw new IllegalArgumentException("Invalid key " + key);
        if (propertiesReadOnly)
          throw new MessageNotWriteableException("Message not writable. attempt to set property " +
              key + " = " + value);
        checkIfStandardProperty(key);
        properties.put(key, value);
    }

    @Override
    public void setLongProperty(String key, long value) throws JMSException {
        if (!MessageUtil.isValidKey(key)) throw new IllegalArgumentException("Invalid key " + key);
        if (propertiesReadOnly)
          throw new MessageNotWriteableException("Message not writable. attempt to set property " +
              key + " = " + value);
        checkIfStandardProperty(key);
        properties.put(key, value);
    }

    @Override
    public void setFloatProperty(String key, float value) throws JMSException {
        if (!MessageUtil.isValidKey(key)) throw new IllegalArgumentException("Invalid key " + key);
        if (propertiesReadOnly)
          throw new MessageNotWriteableException("Message not writable. attempt to set property " +
              key + " = " + value);
        checkIfStandardProperty(key);
        properties.put(key, value);
    }

    @Override
    public void setDoubleProperty(String key, double value) throws JMSException {
        if (!MessageUtil.isValidKey(key)) throw new IllegalArgumentException("Invalid key " + key);
        if (propertiesReadOnly)
          throw new MessageNotWriteableException("Message not writable. attempt to set property " +
              key + " = " + value);
        checkIfStandardProperty(key);
        properties.put(key, value);
    }

    @Override
    public void setStringProperty(String key, String value) throws JMSException {
        if (!MessageUtil.isValidKey(key)) throw new IllegalArgumentException("Invalid key " + key);
        if (propertiesReadOnly)
          throw new MessageNotWriteableException("Message not writable. attempt to set property " +
              key + " = " + value);
        checkIfStandardProperty(key);
        properties.put(key, value);
    }

    @Override
    public void setObjectProperty(String key, Object value) throws JMSException {
        if (!MessageUtil.isValidKey(key)) throw new IllegalArgumentException("Invalid key " + key);
        if (propertiesReadOnly)
          throw new MessageNotWriteableException("Message not writable. attempt to set property " +
              key + " = " + value);
        checkIfStandardProperty(key);

        if (null == value ||
                value instanceof Boolean ||
                value instanceof Byte ||
                value instanceof Short ||
                value instanceof Integer ||
                value instanceof Long ||
                value instanceof Float ||
                value instanceof Double ||
                value instanceof byte[] ||
                value instanceof String) {
            properties.put(key, value);
            return ;        }

        throw new MessageFormatException("Unsupported type for value " + value.getClass());
    }

    // JMS VIOLATION ?
    // I am not sure if getting and setting standard properties is allowed via the generic
    // get/set methods : the spec seems unclear on it.
    // Some javadocs seem to indicate it is NOT allowed. Hence this check ...
    // If it is allowed in JMS - to support it, we will need to have a if/else block within each set/get
    // which delegates to corresponding jms header set/get ...
    private void checkIfStandardProperty(String key) throws JMSException {
        if (standardProperties.contains(key))
          throw new JMSException("Cannot get/set standard JMS properties using *Property api");
    }

    @Override
    public void acknowledge() throws JMSException {
        session.acknowledge(this);
    }

    @Override
    public void clearBody() throws JMSException {
        // Clear the body of the message.
    }

    public String getSourceName() {
        return sourceName;
    }

    public String getSubscriberId() {
        return subscriberId;
    }

    MessageImpl createClone(SessionImpl session, String sourceTopicName, String subscriberId) throws JMSException {
        if (MessageImpl.class != getClass()) {
            throw new JMSException("Unexpected to call MessageImpl's createClone from subclass " + getClass());
        }
        return new MessageImpl(session, this, sourceTopicName, subscriberId);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("MessageImpl");
        sb.append("{session=").append(session);
        sb.append(", jmsMessageId='").append(jmsMessageId).append('\'');
        sb.append(", jmsTimestamp=").append(jmsTimestamp);
        sb.append(", jmsCorrelationID='").append(jmsCorrelationID).append('\'');
        sb.append(", jmsReplyTo=").append(jmsReplyTo);
        sb.append(", jmsDestination=").append(jmsDestination);
        sb.append(", jmsDeliveryMode=").append(jmsDeliveryMode);
        sb.append(", jmsRedelivered=").append(jmsRedelivered);
        sb.append(", jmsType='").append(jmsType).append('\'');
        sb.append(", jmsExpiration=").append(jmsExpiration);
        sb.append(", jmsPriority=").append(jmsPriority);
        sb.append(", properties=").append(properties);
        sb.append(", standardPropertiesExists=").append(standardPropertiesExists);
        sb.append(", standardPropertiesExistsForWire=").append(standardPropertiesExistsForWire);
        sb.append(", sourceName='").append(sourceName).append('\'');
        sb.append(", subscriberId='").append(subscriberId).append('\'');
        sb.append('}');
        return sb.toString();
    }

    void reset() throws JMSException {
        // noop ... children will override to do needful.
    }

    public Runnable getAckRunnable() {
        return ackRunnable;
    }
}
