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
package org.apache.hedwig.jms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.InvalidClientIDException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.ServerSessionPool;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Implementation of jmx Connection
 * MUST be MT-safe (2.8)
 */
public abstract class ConnectionImpl implements Connection, TopicConnection, QueueConnection {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionImpl.class);

    // Copied from old HedwigConnectoin
    // TODO move to a constants class?
    public static final String HEDWIG_CLIENT_CONFIG_FILE = "hedwig.client.config.file";
    private static final Set<String> globalClientIdSet = new HashSet<String>(16);

    private final String user;
    private final String password;

    // Will be set to a random string if unspecified - we do not have a way (currently) for admin
    // specified value ...
    // Note, using the system property means
    // private String clientID = System.getProperty("HEDWIG_CLIENT_ID", null);
    private volatile String clientID = null;

    // I do not like locking on 'this' inspite of the perf diff - allows 'others' to lock on our
    // lock object (client code for example) : leaks MT-safety.
    private final Object lockObject = new Object();

    // Call this when there are issues (primarily connection issues imo).
    // There are two issues with supporting this :
    // a) we do not follow jms model of connection -> multiple sessions; for us, connection does not
    // represent underlying connection to
    // hedwig, but session does. So the basic model does not fit.
    // b) from what I see, hedwig-client does not expose the ability to do this. (it automatically reconnects)
    // hence we do not support this yet.
    private volatile ExceptionListener exceptionListener;

    // connection 'starts' in stopped mode.
    // Until it is started, NO messages MUST be delivered - 4.3.3
    private StateManager connectionState = new StateManager(StateManager.State.STOPPED, lockObject);

    private final List<SessionImpl> sessionList = new ArrayList<SessionImpl>(4);
    private final ConnectionMetaData metadata = new ConnectionMetaDataImpl();

    protected ConnectionImpl() {
        this.user = null;
        this.password = null;
    }

    protected ConnectionImpl(String user, String password) {
        this.user = user;
        this.password = password;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SessionImpl createSession(boolean transacted, int acknowledgeMode)  throws JMSException {
        return createSessionImpl(transacted, acknowledgeMode, null);
    }

    protected SessionImpl createSessionImpl(boolean transacted, int acknowledgeMode,
                                            MessagingSessionFacade.DestinationType type)  throws JMSException {
        final boolean needStart;
        synchronized (lockObject){
            if (connectionState.isInCloseMode())
              throw new javax.jms.IllegalStateException("Connection closed");
            if (connectionState.isTransitionState()) {
                connectionState.waitForTransientStateChange(StateManager.WAIT_TIME_FOR_TRANSIENT_STATE_CHANGE, logger);
                // Not expected actually, present to guard against future changes ...
                if (connectionState.isTransitionState())
                  throw new JMSException("Connection did not make state change to steady state ?");

                if (connectionState.isClosed()) throw new JMSException("Connection already closed");

            }

            assert StateManager.State.STOPPED == connectionState.getCurrentState() ||
                    StateManager.State.STARTED == connectionState.getCurrentState();

            // create within lock, so that it can register with connection, etc ...
            // session = new SessionImpl(this, transacted, acknowledgeMode);
            final SessionImpl session = createSessionInstance(transacted, acknowledgeMode, type);
            sessionList.add(session);
            needStart = connectionState.isStarted();

            if (needStart) session.start();
            return session;
        }
    }

    protected abstract SessionImpl createSessionInstance(boolean transacted, int acknowledgeMode,
                                                         MessagingSessionFacade.DestinationType type)
        throws JMSException;

    public boolean removeSession(SessionImpl session){
        // simply remove.
        synchronized (lockObject){
            return sessionList.remove(session);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getClientID() {
        return clientID;
    }

    /**
     * Allow clientID to be set only if it is NOT administratively configured for connection. (4.3.2)
     * Since we do not work (yet) within the context of a container, this aspect is a TODO for now.
     *
     * {@inheritDoc}
     */
    @Override
    public void setClientID(String clientID) throws InvalidClientIDException, javax.jms.IllegalStateException {
        if (null == clientID) throw new InvalidClientIDException("clientId specified is null");

        synchronized (globalClientIdSet){
            if (globalClientIdSet.contains(clientID))
              throw new InvalidClientIDException("clientId '" + clientID + "' already in use in this provider");
            if (null != this.clientID && !this.clientID.equals(clientID))
              throw new javax.jms.IllegalStateException("clientID already set to " + this.clientID +
                  ", cant override to " + clientID);
            this.clientID = clientID;
            globalClientIdSet.add(clientID);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        // return new ConnectionMetaDataImpl();
        return metadata;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        return exceptionListener;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setExceptionListener(ExceptionListener exceptionListener) throws JMSException {
        this.exceptionListener = exceptionListener;
    }

    public void initConnectionClientID() throws javax.jms.IllegalStateException, InvalidClientIDException {
        synchronized (lockObject){
            // default to hedwig_client_id ?
            if (null == clientID) setClientID(SessionImpl.generateRandomString());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() throws JMSException {

        final StateManager.State prevState;
        final List<SessionImpl> sessionListCopy;

        if (logger.isTraceEnabled()) logger.trace("Attempting to start connection");

        synchronized (lockObject){
            if (connectionState.isStarted()) return ;
            if (connectionState.isClosed()) throw new JMSException("Connection already closed");

            if (connectionState.isTransitionState()){
                connectionState.waitForTransientStateChange(StateManager.WAIT_TIME_FOR_TRANSIENT_STATE_CHANGE, logger);
                // Not expected actually, present to guard against future changes ...
                if (connectionState.isTransitionState())
                  throw new JMSException("Connection did not make state change to steady state ?");

                if (connectionState.isClosed()) throw new JMSException("Connection already closed");
                if (connectionState.isStarted()) return ;

                assert connectionState.isStopped();
                // try again ...
            }

            prevState = connectionState.getCurrentState();
            connectionState.setCurrentState(StateManager.State.STARTING);
            initConnectionClientID();
            sessionListCopy = new ArrayList<SessionImpl>(sessionList);
        }

        StateManager.State nextState = prevState;
        try {
            // There will be only one thread down here ...

            // start any provider specific implementation bootstrap ...
            doStart(user, password);
            // Copy to prevent concurrent mod exceptions.
            if (logger.isTraceEnabled()) logger.trace("Starting " + sessionListCopy.size() + " sessions");
            for (SessionImpl session : sessionListCopy) {
                try {
                    session.start();
                } catch (JMSException jex){
                    // log the error and ignore
                    if (logger.isInfoEnabled()) logger.info("exception starting session : " + jex);
                    DebugUtil.dumpJMSStacktrace(logger, jex);
                }
            }
            nextState = StateManager.State.STARTED;
        } finally {
            // set status and notify.
            synchronized (lockObject){
                connectionState.setCurrentState(nextState);
                lockObject.notifyAll();
            }
        }
    }


    protected String getUser(){
        return user;
    }
    protected String getPassword(){
        return password;
    }

    protected abstract void doStart(String user, String password) throws JMSException;
    protected abstract void doStop();
    protected abstract void doClose();

    @Override
    public void stop() throws JMSException {

        final StateManager.State prevState;
        final List<SessionImpl> sessionListCopy;

        if (logger.isTraceEnabled()) logger.trace("Attempting to stop connection");

        synchronized (lockObject){
            if (connectionState.isClosed()) throw new JMSException("Already closed");
            if (connectionState.isStopped()) return ;

            if (connectionState.isTransitionState()){
                connectionState.waitForTransientStateChange(StateManager.WAIT_TIME_FOR_TRANSIENT_STATE_CHANGE, logger);
                // Not expected actually, present to guard against future changes ...
                if (connectionState.isTransitionState())
                  throw new JMSException("Connection did not make state change to steady state ?");

                if (connectionState.isClosed()) throw new JMSException("Already closed");
                if (connectionState.isStopped()) return ;

                assert connectionState.isStarted();

                // try (again ?) ...
            }

            prevState = connectionState.getCurrentState();
            connectionState.setCurrentState(StateManager.State.STOPPING);
            sessionListCopy = new ArrayList<SessionImpl>(sessionList);
        }

        StateManager.State nextState = prevState;
        try {
            // In case there are any specific changes to be done.
            // Copy to prevent concurrent mod exceptions.

            // Stop all sessions - doing this within MT-safe block to prevent any possibility of race conditions.
            // Potentially expensive, but it is a tradeoff between correctness and performance :-(
            if (logger.isTraceEnabled()) logger.trace("Stopping " + sessionListCopy.size() + " sessions");
            for (SessionImpl session : sessionListCopy) {
                try {
                    session.stop();
                } catch (JMSException jex){
                    // log the error and ignore
                    if (logger.isInfoEnabled()) logger.info("exception closing session : " + jex);
                    DebugUtil.dumpJMSStacktrace(logger, jex);
                }
            }
            // stop connection AFTER session's are stopped.
            doStop();
            nextState = StateManager.State.STOPPED;
        } finally {
            // set status and notify.
            synchronized (lockObject){
                lockObject.notifyAll();
                connectionState.setCurrentState(nextState);
            }
        }
    }

    @Override
    public void close() throws JMSException {

        final StateManager.State prevState;
        final List<SessionImpl> sessionListCopy;

        if (logger.isTraceEnabled()) logger.trace("Attempting to close connection");

        synchronized (lockObject){
            if (connectionState.isClosed()) return ;
            if (! connectionState.isStopped()) {
                if (connectionState.isTransitionState()){
                    connectionState.waitForTransientStateChange(
                            StateManager.WAIT_TIME_FOR_TRANSIENT_STATE_CHANGE, logger);
                    // Not expected actually, present to guard against future changes ...
                    if (connectionState.isTransitionState())
                      throw new JMSException("Connection did not make state change to steady state ?");

                    if (connectionState.isClosed()) return ;

                    assert connectionState.isStarted() || connectionState.isStopped();
                }
            }

            prevState = connectionState.getCurrentState();
            connectionState.setCurrentState(StateManager.State.CLOSING);
            sessionListCopy = new ArrayList<SessionImpl>(sessionList);
        }

        StateManager.State nextState = prevState;

        try {
            // Copy to prevent concurrent mod exceptions.

            // Close all sessions - doing this within MT-safe block to prevent any possibility of race conditions.
            // Potentially expensive, but it is a tradeoff between correctness and performance :-(
            if (logger.isTraceEnabled()) logger.trace("Closing " + sessionListCopy.size() + " sessions");
            for (SessionImpl session : sessionListCopy) {
                try {
                    session.close();
                } catch (Exception ex){
                    // log the error and ignore
                    if (logger.isDebugEnabled()) logger.debug("exception closing session", ex);
                    else if (logger.isInfoEnabled()) logger.info("exception closing session : " + ex);
                }
            }
            doClose();
            synchronized (globalClientIdSet){
                assert (null != getClientID());
                globalClientIdSet.remove(getClientID());
            }
            nextState = StateManager.State.CLOSED;
        } finally {
            // set status and notify.
            synchronized (lockObject){
                lockObject.notifyAll();

                if (StateManager.State.CLOSED == nextState && !connectionState.isClosed()) {
                    // clear sessions.
                    sessionList.clear();
                }
                // set after everything is done.
                connectionState.setCurrentState(nextState);
            }
        }
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector,
                                                       ServerSessionPool sessionPool, int maxMessages)
        throws JMSException {

        throw new JMSException("Unsupported");
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName,
                                                              String messageSelector, ServerSessionPool sessionPool,
                                                              int maxMessages)
        throws JMSException {

        throw new JMSException("Unsupported");
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Queue queue, String s,
                                                       ServerSessionPool serverSessionPool, int maxMessages)
        throws JMSException {

        throw new JMSException("Unsupported");
    }


    @Override
    public ConnectionConsumer createConnectionConsumer(Topic topic, String messageSelector,
                                                       ServerSessionPool sessionPool, int maxMessages)
        throws JMSException {

        throw new JMSException("Unsupported");
    }

    public boolean isInStartMode() {
        return connectionState.isInStartMode();
    }

    protected abstract MessagingSessionFacade createMessagingSessionFacade(SessionImpl session) throws JMSException;


    // required to catch resource leaks ...
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!connectionState.isClosed()) {
            if (logger.isErrorEnabled()) logger.error("Connection was NOT closed before it went out of scope");
            close();
        }
    }

    private static final int LOCALLY_SENT_MESSAGE_ID_CACHE_SIZE =
        Integer.getInteger("LOCALLY_SENT_MESSAGE_ID_CACHE_SIZE", 1024);
    // This is gaurded by publishedMessageIds
    private final LRUCacheSet<String> publishedMessageIds =
        new LRUCacheSet<String>(LOCALLY_SENT_MESSAGE_ID_CACHE_SIZE, true);

    boolean isLocallyPublished(String messageId){

        if (null == messageId) return false;

        synchronized(publishedMessageIds){
            return publishedMessageIds.contains(messageId);
        }
    }

    void addToLocallyPublishedMessageIds(String messageId){

        if (null == messageId) return ;

        synchronized(publishedMessageIds){
            publishedMessageIds.add(messageId);
        }
    }
}
