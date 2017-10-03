/*
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
package org.apache.bookkeeper.common.concurrent;

import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;

/**
 * Provide similar interface (as twitter future) over java future.
 */
public interface FutureEventListener<T> extends BiConsumer<T, Throwable> {

  void onSuccess(T value);

  void onFailure(Throwable cause);

  @Override
  default void accept(T t, Throwable throwable) {
    if (null != throwable) {
      if (throwable instanceof CompletionException && null != throwable.getCause()) {
        onFailure(throwable.getCause());
      } else {
        onFailure(throwable);
      }
      return;
    }
    onSuccess(t);
  }
}
