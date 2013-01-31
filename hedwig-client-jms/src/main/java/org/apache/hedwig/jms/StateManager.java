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

import javax.jms.JMSException;


/**
    Specific to jms package - NOT to be used elsewhere.

    The util class allows for managing the current readiness 'state' of the object which hosts it
    along the axis of StateManager.State while being MT safe. Right now, both Connection and Session make use of it.

    The lockObject is used to do timed wait's (which the host object will notify on) in case of async state changes.


    This is not a general purpose code, but specific to state transitions mentioned in jms spec.


    All use of the class goes like this :


    StateManager.State prevState;
    acquire lock:
        if in transition state, wait.
        if in expected state, return.
        if in error state, return/throw exception.
        if in valid state transition state -
            prevState = currentState.
            set to corresponding transition state (STARTING, CLOSING, etc).
        Other method specific changes.
    release lock:

    nextState = prevState (in case state change failed, revert).

    try {
        attempt state change.
        on success nextState = next valid state for this method.
    } finally {
        acquire lock:
          change state to nextState
        release lock:
    }

    * So at any given point of time, the state will be in transition ONLY when there is an attempt
            being made to transition.
    * The state will always be in a final state at all other points of time.
    * No attempt will be made to change state while a transition state is currently in progress.
 */
final class StateManager {
  public static final long WAIT_TIME_FOR_TRANSIENT_STATE_CHANGE =
      Long.getLong("WAIT_TIME_FOR_TRANSIENT_STATE_CHANGE", 16000L);

  static enum State {
        STARTING(true, false, true),
        STARTED(true, false, false),
        STOPPING(false, false, true),
        STOPPED(false, false, false),
        CLOSING(false, true, true),
        CLOSED(false, true, false);

        private final boolean inStartMode;
        private final boolean inCloseMode;
        private final boolean inTransitionMode;

        State(boolean inStartMode, boolean inCloseMode, boolean inTransitionMode) {
            this.inStartMode = inStartMode;
            this.inCloseMode = inCloseMode;
            this.inTransitionMode = inTransitionMode;
        }

        public boolean isInStartMode() {
            return inStartMode;
        }

        public boolean isInCloseMode() {
            return inCloseMode;
        }

        public boolean isInTransitionMode() {
            return inTransitionMode;
        }
    }

    // DO NOT do something silly like State.STARTING == currentState || State.STARTED == currentState, etc !
    private volatile State currentState;
    private final Object lockObject;

    StateManager(State startStart, Object lockObject){
        this.currentState = startStart;
        this.lockObject = lockObject;
    }

    State getCurrentState() {
        return currentState;
    }

    boolean isStarted() {
        return State.STARTED == currentState;
    }

    boolean isInStartMode() {
        return currentState.isInStartMode();
    }

    boolean isStopped() {
        return State.STOPPED == currentState;
    }

    boolean isClosed() {
        return State.CLOSED == currentState;
    }

    // NOT locking explicitly : typically, already locked on lockObject
    boolean isInCloseMode() {
        return currentState.isInCloseMode();
    }

    // NOT locking explicitly : typically, already locked on lockObject
    boolean isTransitionState() {
        return currentState.isInTransitionMode();
    }

    void setCurrentState(State currentState) {
        this.currentState = currentState;
    }

    // NOT locking explicitly : MUST be already locked on lockObject
    void waitForTransientStateChange(long timeout, Logger logger) throws JMSException {
        final long startTime = SessionImpl.currentTimeMillis();
        final int WAIT_UNIT = 100;
        int retryCount = (int)(timeout / WAIT_UNIT);

        while (isTransitionState()) {
            try {
                // If we are NOT locked on lockObject, this will throw exception !
                lockObject.wait(WAIT_UNIT);
            } catch (InterruptedException e) {
                // bubble it up.
                JMSException jex = new JMSException("Thread interrupted ... " + e);
                jex.setLinkedException(e);
                throw jex;
            }
            retryCount --;
            if (retryCount <= 0) {
                if (logger.isDebugEnabled()) DebugUtil.dumpAllStacktraces(logger);
                // throw new JMSException("wait timeout " + (SessionImpl.currentTimeMillis() - startTime));
                throw new JMSException("wait for " + (SessionImpl.currentTimeMillis() - startTime) + " timeout");
            }
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("StateManager");
        sb.append("{currentState=").append(currentState);
        sb.append('}');
        return sb.toString();
    }
}
