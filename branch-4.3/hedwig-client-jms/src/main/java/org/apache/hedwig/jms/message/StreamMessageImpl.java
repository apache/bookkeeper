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
import org.apache.hedwig.jms.Mutable;
import org.apache.hedwig.jms.SessionImpl;
import org.apache.hedwig.protocol.PubSubProtocol;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageEOFException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;
import javax.jms.StreamMessage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Map;

/**
 * Though similar to BytesMessageImpl, the difference is that BytesMessage expects the user to know
 * the schema while
 * StreamMessage user expects type conversion, etc.
 *
 * In our case, the stream is not a true open stream to the server; it is buffered in memory.
 */
public class StreamMessageImpl extends MessageImpl implements StreamMessage {
    private ReadOnlyMessage readOnlyMessage;
    private WriteOnlyMessage writeOnlyMessage;
    private boolean readMode;

    public StreamMessageImpl(SessionImpl session) throws JMSException {
        super(session);
        clearBody();
    }

    // To clone a message
    public StreamMessageImpl(SessionImpl session, StreamMessageImpl message, String sourceTopicName,
                             String subscriberId) throws JMSException {
        super(session, (MessageImpl) message, sourceTopicName, subscriberId);
        try {
            if (message.readMode){
                this.readOnlyMessage = new ReadOnlyMessage(message.getPayloadData());
                this.writeOnlyMessage = null;
            }
            else {
                this.readOnlyMessage = null;
                this.writeOnlyMessage = new WriteOnlyMessage(message.getPayloadData());
            }
        } catch (IOException e) {
            JMSException ex = new JMSException("Unable to clone/copy input message " + message + " .. " + e);
            ex.setLinkedException(e);
            throw ex;
        }

        this.readMode = message.readMode;
    }

    // To clone a message from a StreamMessage which is NOT StreamMessageImpl
    // Changing order of parameter to NOT accidentally clash with the constructor above.
    // This is midly confusing, but helps a lot in preventing accidental bugs !
    public StreamMessageImpl(StreamMessage message, SessionImpl session) throws JMSException {
        super((Message) message, session);

        if (message instanceof StreamMessageImpl) {
            throw new JMSException("Coding bug - should use this constructor ONLY for non StreamMessageImpl messages");
        }

        final byte[] data;
        try {
            WriteOnlyMessage wom = new WriteOnlyMessage();
            try {
                Object obj;
                while (null != (obj = message.readObject())){
                    wom.writeObject(obj);
                }
            } catch (EOFException eof){
                // ignore ...
            }
            data = wom.getPayloadAsBytes(null);
        } catch (IOException e) {
            JMSException jEx = new JMSException("Unable to write to internal message .. " + e);
            jEx.setLinkedException(e);
            throw jEx;
        }

        this.writeOnlyMessage = new WriteOnlyMessage(data);

        this.readOnlyMessage  = null;
        this.readMode = false;
    }

    StreamMessageImpl(SessionImpl session, PubSubProtocol.Message message, Map<String, Object> properties,
                      String sourceTopicName, String subscriberId, Runnable ackRunnable) throws JMSException {
        super(session, message, properties, sourceTopicName, subscriberId, ackRunnable);

        final byte[] data = message.getBody().toByteArray();
        try {
            this.readOnlyMessage = new ReadOnlyMessage(data);
        } catch (IOException e) {
            JMSException ex = new JMSException("Unable to clone/copy input message " + message + " .. " + e);
            ex.setLinkedException(e);
            throw ex;
        }

        this.writeOnlyMessage = null;
        this.readMode = true;
    }

    @Override
    protected MessageUtil.SupportedMessageTypes getJmsMessageType() {
        return MessageUtil.SupportedMessageTypes.STREAM;
    }

    protected boolean isBodyEmpty(){
        return false;
    }

    @Override
    public PubSubProtocol.Message generateHedwigMessage() throws JMSException {
        PubSubProtocol.Message.Builder builder = PubSubProtocol.Message.newBuilder();
        super.populateBuilderWithHeaders(builder);

        // Now set body and type.
        try {
            byte[] data = getPayloadData();
            builder.setBody(ByteString.copyFrom(data));
        } catch (IOException e) {
            JMSException ex = new JMSException("Unable to read message data .. " + e);
            ex.setLinkedException(e);
            throw ex;
        }

        return builder.build();
    }


    @Override
    public boolean readBoolean() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readBoolean();
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("ioEx ?");
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public byte readByte() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readByte();
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("ioEx ?");
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public short readShort() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readShort();
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("ioEx ?");
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public char readChar() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readChar();
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("ioEx ?");
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public int readInt() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readInt();
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("ioEx ?");
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public long readLong() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readLong();
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("ioEx ?");
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public float readFloat() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readFloat();
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("ioEx ?");
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public double readDouble() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readDouble();
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("ioEx ?");
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public String readString() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readString();
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("ioEx ?");
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public int readBytes(byte[] data) throws JMSException {
        throw new UnsupportedOperationException("Please use readObject - this method is not supported");
    }

    @Override
    public Object readObject() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readObject();
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("ioEx ?");
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public void writeBoolean(boolean val) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        try {
            writeOnlyMessage.writeBoolean(val);
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("Unexpected ioex : " + ioEx);
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public void writeByte(byte val) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        try {
            writeOnlyMessage.writeByte(val);
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("Unexpected ioex : " + ioEx);
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public void writeShort(short val) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        try {
            writeOnlyMessage.writeShort(val);
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("Unexpected ioex : " + ioEx);
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public void writeChar(char val) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        try {
            writeOnlyMessage.writeChar(val);
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("Unexpected ioex : " + ioEx);
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public void writeInt(int val) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        try {
            writeOnlyMessage.writeInt(val);
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("Unexpected ioex : " + ioEx);
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public void writeLong(long val) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        try {
            writeOnlyMessage.writeLong(val);
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("Unexpected ioex : " + ioEx);
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public void writeFloat(float val) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        try {
            writeOnlyMessage.writeFloat(val);
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("Unexpected ioex : " + ioEx);
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public void writeDouble(double val) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        try {
            writeOnlyMessage.writeDouble(val);
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("Unexpected ioex : " + ioEx);
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public void writeString(String val) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        try {
            writeOnlyMessage.writeString(val);
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("Unexpected ioex : " + ioEx);
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public void writeBytes(byte[] data) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        try {
            writeOnlyMessage.writeBytes(data);
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("Unexpected ioex : " + ioEx);
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public void writeBytes(byte[] data, int offset, int length) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        try {
            writeOnlyMessage.writeBytes(data, offset, length);
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("Unexpected ioex : " + ioEx);
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    // This method is ONLY supposed to be used for object form of primitive types !
    @Override
    public void writeObject(Object obj) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        try {
            writeOnlyMessage.writeObject(obj);
        } catch (IOException ioEx){
            JMSException eofEx = new JMSException("Unexpected ioex : " + ioEx);
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public void reset() throws JMSException {
        if (this.readMode) return ;
        this.readMode = true;
        try {
            byte[] data = writeOnlyMessage.getPayloadAsBytes(null);
            this.readOnlyMessage = new ReadOnlyMessage(data);
        } catch (IOException e) {
            JMSException ex = new JMSException("cant convert to read only message ... unexpected actually .. " + e);
            ex.setLinkedException(e);
            throw ex;
        }
        this.writeOnlyMessage = null;
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        this.writeOnlyMessage = new WriteOnlyMessage();
        this.readOnlyMessage = null;
        this.readMode = false;
    }

    private byte[] getPayloadData() throws IOException, IllegalStateException {
        if (readMode) return readOnlyMessage.getDataCopy();

        Mutable<byte[]> preCloseData = new Mutable<byte[]>(null);
        byte[] data = writeOnlyMessage.getPayloadAsBytes(preCloseData);

        writeOnlyMessage = new WriteOnlyMessage(preCloseData.getValue());
        return data;
    }


    @Override
    StreamMessageImpl createClone(SessionImpl session, String sourceTopicName, String subscriberId)
        throws JMSException {
        return new StreamMessageImpl(session, this, sourceTopicName, subscriberId);
    }

    // Using java object's instead of primitives to avoid having to store schema separately.
    private static class ReadOnlyMessage {

        private final ObjectInputStream ois;
        private final byte[] data;
        private final Deque<Object> unreadObjects = new ArrayDeque<Object>(4);

        public ReadOnlyMessage(byte[] data) throws IOException {
            this.data = data;
            this.ois = new ObjectInputStream(new ByteArrayInputStream(data));
        }

        public byte[] getDataCopy(){
            return Arrays.copyOf(data, data.length);
        }

        private void unreadObject(Object obj) {
            unreadObjects.push(obj);
        }

        private Object readNextObject() throws IOException, JMSException {
            try {
                if (! unreadObjects.isEmpty()) return unreadObjects.pop();

                return ois.readObject();
            } catch (ClassNotFoundException e) {
                // unexpected !
                javax.jms.IllegalStateException jEx =
                    new javax.jms.IllegalStateException("Unexpected not to be able to resolve class");
                jEx.setLinkedException(e);
                throw jEx;
            } catch (EOFException eof) {
                throw new MessageEOFException("eof");
            }
        }

        public boolean readBoolean() throws IOException, JMSException {
            Object obj = readNextObject();
            boolean failed = true;
            try {
                Boolean value = MessageUtil.asBoolean(obj);
                failed = false;
                return value;
            } finally {
                if (failed) unreadObject(obj);
            }
        }

        public byte readByte() throws IOException, JMSException {
            Object obj = readNextObject();
            boolean failed = true;
            try {
                Byte value = MessageUtil.asByte(obj);
                failed = false;
                return value;
            } finally {
                if (failed) unreadObject(obj);
            }
        }

        public short readShort() throws IOException, JMSException {
            Object obj = readNextObject();
            boolean failed = true;
            try {
                Short value = MessageUtil.asShort(obj);
                failed = false;
                return value;
            } finally {
                if (failed) unreadObject(obj);
            }
        }

        public char readChar() throws IOException, JMSException {
            Object obj = readNextObject();
            boolean failed = true;
            try {
                Character value = MessageUtil.asChar(obj);
                failed = false;
                return value;
            } finally {
                if (failed) unreadObject(obj);
            }
        }

        public int readInt() throws IOException, JMSException {
            Object obj = readNextObject();
            boolean failed = true;
            try {
                Integer value = MessageUtil.asInteger(obj);
                failed = false;
                return value;
            } finally {
                if (failed) unreadObject(obj);
            }
        }

        public long readLong() throws IOException, JMSException {
            Object obj = readNextObject();
            boolean failed = true;
            try {
                Long value = MessageUtil.asLong(obj);
                failed = false;
                return value;
            } finally {
                if (failed) unreadObject(obj);
            }
        }

        public float readFloat() throws IOException, JMSException {
            Object obj = readNextObject();
            boolean failed = true;
            try {
                Float value = MessageUtil.asFloat(obj);
                failed = false;
                return value;
            } finally {
                if (failed) unreadObject(obj);
            }
        }

        public double readDouble() throws IOException, JMSException {
            Object obj = readNextObject();
            boolean failed = true;
            try {
                Double value = MessageUtil.asDouble(obj);
                failed = false;
                return value;
            } finally {
                if (failed) unreadObject(obj);
            }
        }

        public String readString() throws IOException, JMSException {
            Object obj = readNextObject();
            boolean failed = true;
            try {
                String value = MessageUtil.asString(obj);
                failed = false;
                return value;
            } finally {
                if (failed) unreadObject(obj);
            }
        }

        public Object readObject() throws IOException, JMSException {
            return readNextObject();
        }
    }

    private static class WriteOnlyMessage {

        private final ByteArrayOutputStream baos;
        // private ObjectOutputStream oos;
        private final ObjectOutputStream oos;

        public WriteOnlyMessage() throws JMSException {
            baos = new ByteArrayOutputStream();
            try {
                oos = new ObjectOutputStream(baos);
            } catch (IOException e) {
                IllegalStateException jEx =
                    new IllegalStateException("Unexpected to not be able to create empty write only message");
                jEx.setLinkedException(e);
                throw jEx;
            }
        }

        private WriteOnlyMessage(final byte[] data) throws IllegalStateException {
            baos = new ByteArrayOutputStream();
            try {
                if (null != data) baos.write(data);
                baos.flush();
                oos = new ObjectOutputStream(baos){
                    // Do not write the header if data is based on already materialized stream.
                    protected void writeStreamHeader() throws IOException {
                        if (null == data || 0 == data.length) super.writeStreamHeader();
                    }
                };
            } catch (IOException e) {
                IllegalStateException jEx =
                    new IllegalStateException("Unexpected to not be able to create empty write only message");
                jEx.setLinkedException(e);
                throw jEx;
            }
        }

        public byte[] getPayloadAsBytes(Mutable<byte[]> preCloseData) throws IOException {
            oos.flush();
            baos.flush();
            if (null != preCloseData) preCloseData.setValue(baos.toByteArray());
            oos.close();
            baos.flush();
            baos.close();
            // oos = null;
            return baos.toByteArray();
        }

        public void writeBoolean(boolean val) throws IOException {
            oos.writeObject(val);
        }

        public void writeByte(byte val) throws IOException {
            oos.writeObject(val);
        }

        public void writeShort(short val) throws IOException {
            oos.writeObject(val);
        }

        public void writeChar(char val) throws IOException {
            oos.writeObject(val);
        }

        public void writeInt(int val) throws IOException {
            oos.writeObject(val);
        }

        public void writeLong(long val) throws IOException {
            oos.writeObject(val);
        }

        public void writeFloat(float val) throws IOException {
            oos.writeObject(val);
        }

        public void writeDouble(double val) throws IOException {
            oos.writeObject(val);
        }

        public void writeString(String val) throws IOException {
            oos.writeObject(val);
        }

        public void writeBytes(byte[] data) throws IOException {
            oos.writeObject(data);
        }

        // copy and write as a single byte array.
        public void writeBytes(byte[] data, int offset, int length) throws IOException {
            byte[] arr = new byte[length];
            System.arraycopy(data, offset, arr, 0, length);
            writeBytes(arr);
        }

        public void writeObject(Object obj) throws JMSException, IOException {
            // unrolling it
            if (obj instanceof Boolean) {
                writeBoolean((Boolean) obj);
            }
            else if (obj instanceof Byte) {
                writeByte((Byte) obj);
            }
            else if (obj instanceof Short) {
                writeShort((Short) obj);
            }
            else if (obj instanceof Character) {
                writeChar((Character) obj);
            }
            else if (obj instanceof Integer) {
                writeInt((Integer) obj);
            }
            else if (obj instanceof Long) {
                writeLong((Long) obj);
            }
            else if (obj instanceof Float) {
                writeFloat((Float) obj);
            }
            else if (obj instanceof Double) {
                writeDouble((Double) obj);
            }
            else if (obj instanceof String) {
                writeString((String) obj);
            }
            else if (obj instanceof byte[]) {
                writeBytes((byte[]) obj);
            }
            else{
                throw new JMSException("Unsupported type for obj : " + obj.getClass());
            }
        }
    }
}