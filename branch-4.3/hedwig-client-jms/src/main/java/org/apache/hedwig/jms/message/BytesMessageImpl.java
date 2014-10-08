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
import org.apache.hedwig.protocol.PubSubProtocol;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageEOFException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * To be used for raw bytes ...
 */
public class BytesMessageImpl extends MessageImpl implements BytesMessage {
    private ReadOnlyMessage readOnlyMessage;
    private WriteOnlyMessage writeOnlyMessage;
    private boolean readMode;

    public BytesMessageImpl(SessionImpl session) throws JMSException {
        super(session);
        clearBody();
    }

    // To clone a message
    public BytesMessageImpl(SessionImpl session, BytesMessageImpl message, String sourceTopicName,
                            String subscriberId) throws JMSException {
        super(session, (MessageImpl) message, sourceTopicName, subscriberId);
        try {
            if (message.readMode){
                this.readOnlyMessage = new ReadOnlyMessage(message.readOnlyMessage.getDataCopy());
                this.writeOnlyMessage = null;
            }
            else {
                this.readOnlyMessage = null;
                this.writeOnlyMessage = new WriteOnlyMessage(message.writeOnlyMessage.getPayloadAsBytes());
            }
        } catch (IOException e) {
            JMSException ex = new JMSException("Unable to clone/copy input message " + message + " .. " + e);
            ex.setLinkedException(e);
            throw ex;
        }
        this.readMode = message.readMode;
    }

    // To clone a message from a BytesMessage which is NOT BytesMessageImpl
    // Changing order of parameter to NOT accidentally clash with the constructor above.
    // This is midly confusing, but helps a lot in preventing accidental bugs !
    public BytesMessageImpl(BytesMessage message, SessionImpl session) throws JMSException {
        super((Message) message, session);

        if (message instanceof BytesMessageImpl) {
            throw new JMSException("Coding bug - should use this constructor ONLY for non " +
                "BytesMessageImpl messages");
        }

        // copy the bytes ...
        final byte[] data;
        {
            final long length = message.getBodyLength();
            if (length < 0 || length >= Integer.MAX_VALUE) throw new JMSException("Unreasonably " +
                "large value for body Length : " + length);

            data = new byte[(int) length];
            int read = 0;
            while (read < length){
                int sz = message.readBytes(data, read);
                read += sz;
            }
        }

        try {
            this.writeOnlyMessage = new WriteOnlyMessage(data);
        } catch (IOException e) {
            JMSException ex = new JMSException("Unable to clone/copy input message " + message + " .. " + e);
            ex.setLinkedException(e);
            throw ex;
        }
        this.readOnlyMessage  = null;
        this.readMode = true;
    }

    public BytesMessageImpl(SessionImpl session, PubSubProtocol.Message message, Map<String, Object> properties,
                            String sourceTopicName, String subscriberId, Runnable ackRunnable) throws JMSException {
        super(session, message, properties, sourceTopicName, subscriberId, ackRunnable);

        this.readOnlyMessage = new ReadOnlyMessage(message.getBody().toByteArray());
        this.writeOnlyMessage = null;
        this.readMode = true;
    }

    @Override
    protected MessageUtil.SupportedMessageTypes  getJmsMessageType() {
        return MessageUtil.SupportedMessageTypes.BYTES;
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
            builder.setBody(ByteString.copyFrom(getPayloadData()));
        } catch (IOException e) {
            JMSException ex = new JMSException("Unable to read message data .. " + e);
            ex.setLinkedException(e);
            throw ex;
        }

        return builder.build();
    }

    @Override
    public long getBodyLength() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        return readOnlyMessage.getBodyLength();
    }

    @Override
    public boolean readBoolean() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readBoolean();
        } catch (IOException eof){
            MessageEOFException eofEx = new MessageEOFException("eof ?");
            eofEx.setLinkedException(eof);
            throw eofEx;
        }
    }

    @Override
    public byte readByte() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readByte();
        } catch (IOException eof){
            MessageEOFException eofEx = new MessageEOFException("eof ?");
            eofEx.setLinkedException(eof);
            throw eofEx;
        }
    }

    @Override
    public int readUnsignedByte() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readUnsignedByte();
        } catch (IOException eof){
            MessageEOFException eofEx = new MessageEOFException("eof ?");
            eofEx.setLinkedException(eof);
            throw eofEx;
        }
    }

    @Override
    public short readShort() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readShort();
        } catch (IOException eof){
            MessageEOFException eofEx = new MessageEOFException("eof ?");
            eofEx.setLinkedException(eof);
            throw eofEx;
        }
    }

    @Override
    public int readUnsignedShort() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readUnsignedShort();
        } catch (IOException eof){
            MessageEOFException eofEx = new MessageEOFException("eof ?");
            eofEx.setLinkedException(eof);
            throw eofEx;
        }
    }

    @Override
    public char readChar() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readChar();
        } catch (IOException eof){
            MessageEOFException eofEx = new MessageEOFException("eof ?");
            eofEx.setLinkedException(eof);
            throw eofEx;
        }
    }

    @Override
    public int readInt() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readInt();
        } catch (IOException eof){
            MessageEOFException eofEx = new MessageEOFException("eof ?");
            eofEx.setLinkedException(eof);
            throw eofEx;
        }
    }

    @Override
    public long readLong() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readLong();
        } catch (IOException eof){
            MessageEOFException eofEx = new MessageEOFException("eof ?");
            eofEx.setLinkedException(eof);
            throw eofEx;
        }
    }

    @Override
    public float readFloat() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readFloat();
        } catch (IOException eof){
            MessageEOFException eofEx = new MessageEOFException("eof ?");
            eofEx.setLinkedException(eof);
            throw eofEx;
        }
    }

    @Override
    public double readDouble() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readDouble();
        } catch (IOException eof){
            MessageEOFException eofEx = new MessageEOFException("eof ?");
            eofEx.setLinkedException(eof);
            throw eofEx;
        }
    }

    @Override
    public String readUTF() throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readUTF();
        } catch (IOException eof){
            MessageEOFException eofEx = new MessageEOFException("eof ?");
            eofEx.setLinkedException(eof);
            throw eofEx;
        }
    }

    @Override
    public int readBytes(byte[] data) throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readBytes(data);
        } catch (IOException eof){
            MessageEOFException eofEx = new MessageEOFException("eof ?");
            eofEx.setLinkedException(eof);
            throw eofEx;
        }
    }

    @Override
    public int readBytes(byte[] data, int length) throws JMSException {
        if (!readMode) throw new MessageNotReadableException("Message not readable");
        try {
            return readOnlyMessage.readBytes(data, length);
        } catch (IOException eof){
            MessageEOFException eofEx = new MessageEOFException("eof ?");
            eofEx.setLinkedException(eof);
            throw eofEx;
        }
    }

    @Override
    public void writeBoolean(boolean val) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        try {
            writeOnlyMessage.writeBoolean(val);
        } catch (IOException ioEx){
            MessageEOFException eofEx = new MessageEOFException("Unexpected ioex : " + ioEx);
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
            MessageEOFException eofEx = new MessageEOFException("Unexpected ioex : " + ioEx);
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
            MessageEOFException eofEx = new MessageEOFException("Unexpected ioex : " + ioEx);
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
            MessageEOFException eofEx = new MessageEOFException("Unexpected ioex : " + ioEx);
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
            MessageEOFException eofEx = new MessageEOFException("Unexpected ioex : " + ioEx);
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
            MessageEOFException eofEx = new MessageEOFException("Unexpected ioex : " + ioEx);
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
            MessageEOFException eofEx = new MessageEOFException("Unexpected ioex : " + ioEx);
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
            MessageEOFException eofEx = new MessageEOFException("Unexpected ioex : " + ioEx);
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public void writeUTF(String val) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        try {
            writeOnlyMessage.writeUTF(val);
        } catch (IOException ioEx){
            MessageEOFException eofEx = new MessageEOFException("Unexpected ioex : " + ioEx);
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
            MessageEOFException eofEx = new MessageEOFException("Unexpected ioex : " + ioEx);
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
            MessageEOFException eofEx = new MessageEOFException("Unexpected ioex : " + ioEx);
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    // This method is ONLY supposed to be used for object form of primitive types !
    @Override
    public void writeObject(Object obj) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        try {
            // unrolling it
            if (obj instanceof Boolean) {
                writeOnlyMessage.writeBoolean((Boolean) obj);
            }
            else if (obj instanceof Byte) {
                writeOnlyMessage.writeByte((Byte) obj);
            }
            else if (obj instanceof Short) {
                writeOnlyMessage.writeShort((Short) obj);
            }
            else if (obj instanceof Character) {
                writeOnlyMessage.writeChar((Character) obj);
            }
            else if (obj instanceof Integer) {
                writeOnlyMessage.writeInt((Integer) obj);
            }
            else if (obj instanceof Long) {
                writeOnlyMessage.writeLong((Long) obj);
            }
            else if (obj instanceof Float) {
                writeOnlyMessage.writeFloat((Float) obj);
            }
            else if (obj instanceof Double) {
                writeOnlyMessage.writeDouble((Double) obj);
            }
            else if (obj instanceof String) {
                writeOnlyMessage.writeUTF((String) obj);
            }
            else if (obj instanceof byte[]) {
                writeOnlyMessage.writeBytes((byte[]) obj);
            }
            else{
                throw new JMSException("Unsupported type for obj : " + obj.getClass());
            }
        } catch (IOException ioEx){
            MessageEOFException eofEx = new MessageEOFException("Unexpected ioex : " + ioEx);
            eofEx.setLinkedException(ioEx);
            throw eofEx;
        }
    }

    @Override
    public void reset() throws JMSException {
        if (this.readMode) return ;
        try {
            this.readOnlyMessage = new ReadOnlyMessage(writeOnlyMessage.getPayloadAsBytes());
        } catch (IOException e) {
            JMSException ex = new JMSException("Unable to convert write-only message to read-only message .. " + e);
            ex.setLinkedException(e);
            throw ex;
        }
        this.readMode = true;
        this.writeOnlyMessage = null;
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        this.writeOnlyMessage = new WriteOnlyMessage();
        this.readOnlyMessage = null;
        this.readMode = false;
    }

    private byte[] getPayloadData() throws IOException {
        if (readMode) return readOnlyMessage.getDataCopy();
        return writeOnlyMessage.getPayloadAsBytes();
    }

    @Override
    BytesMessageImpl createClone(SessionImpl session, String sourceTopicName, String subscriberId)
        throws JMSException {
        return new BytesMessageImpl(session, this, sourceTopicName, subscriberId);
    }

    // Using java object's instead of primitives to avoid having to store schema separately.
    private static class ReadOnlyMessage {

        private final DataInputStream dis;
        private final byte[] data;

        public ReadOnlyMessage(byte[] data) {
            this.dis = new DataInputStream(new ByteArrayInputStream(data));
            this.data = data;
        }

        public byte[] getDataCopy(){
            byte[] retval = new byte[data.length];
            System.arraycopy(data, 0, retval, 0, retval.length);
            return retval;
        }

        public int getBodyLength() {
            return data.length;
        }

        public boolean readBoolean() throws IOException {
            return dis.readBoolean();
        }

        public byte readByte() throws IOException {
            return dis.readByte();
        }

        public int readUnsignedByte() throws IOException {
            return dis.readUnsignedByte();
        }

        public short readShort() throws IOException {
            return dis.readShort();
        }

        public int readUnsignedShort() throws IOException {
            return dis.readUnsignedShort();
        }

        public char readChar() throws IOException {
            return dis.readChar();
        }

        public int readInt() throws IOException {
            return dis.readInt();
        }

        public long readLong() throws IOException {
            return dis.readLong();
        }

        public float readFloat() throws IOException {
            return dis.readFloat();
        }

        public double readDouble() throws IOException {
            return dis.readDouble();
        }

        public String readUTF() throws IOException {
            return dis.readUTF();
        }

        public int readBytes(byte[] data) throws IOException {
            return dis.read(data);
        }

        public int readBytes(byte[] data, int length) throws IOException {
            if (length < 0 || length > data.length)
              throw new IndexOutOfBoundsException("Invalid length specified : " + length + ", data : " + data.length);
            return dis.read(data, 0, length);
        }
    }

    private static class WriteOnlyMessage {

        private final ByteArrayOutputStream baos;
        private final DataOutputStream dos;

        public WriteOnlyMessage(){
            baos = new ByteArrayOutputStream();
            dos = new DataOutputStream(baos);
        }

        public WriteOnlyMessage(byte[] data) throws IOException {
            baos = new ByteArrayOutputStream();
            dos = new DataOutputStream(baos);
            dos.write(data);
        }

        public byte[] getPayloadAsBytes() throws IOException {
            dos.flush();
            return baos.toByteArray();
        }

        public void writeBoolean(boolean val) throws IOException {
            dos.writeBoolean(val);
        }

        public void writeByte(byte val) throws IOException {
            dos.writeByte(val);
        }

        public void writeShort(short val) throws IOException {
            dos.writeShort(val);
        }

        public void writeChar(char val) throws IOException {
            dos.writeChar(val);
        }

        public void writeInt(int val) throws IOException {
            dos.writeInt(val);
        }

        public void writeLong(long val) throws IOException {
            dos.writeLong(val);
        }

        public void writeFloat(float val) throws IOException {
            dos.writeFloat(val);
        }

        public void writeDouble(double val) throws IOException {
            dos.writeDouble(val);
        }

        public void writeUTF(String val) throws IOException {
            dos.writeUTF(val);
        }

        public void writeBytes(byte[] data) throws IOException {
            dos.write(data);
        }

        public void writeBytes(byte[] data, int offset, int length) throws IOException {
            dos.write(data, offset, length);
        }
    }
}