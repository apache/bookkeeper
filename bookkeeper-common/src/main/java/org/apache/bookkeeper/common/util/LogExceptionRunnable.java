/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.common.util;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Throwables;
import lombok.extern.slf4j.Slf4j;

/**
 * A simple wrapper for a {@link Runnable} that logs any exception thrown by it, before
 * re-throwing it.
 */
@Slf4j
public final class LogExceptionRunnable implements Runnable {

  private final Runnable task;

  public LogExceptionRunnable(Runnable task) {
    this.task = checkNotNull(task);
  }

  @Override
  public void run() {
    try {
      task.run();
    } catch (Throwable t) {
      log.error("Exception while executing runnable " + task, t);
      Throwables.throwIfUnchecked(t);
      throw new AssertionError(t);
    }
  }

  @Override
  public String toString() {
    return "LogExceptionRunnable(" + task + ")";
  }
}
