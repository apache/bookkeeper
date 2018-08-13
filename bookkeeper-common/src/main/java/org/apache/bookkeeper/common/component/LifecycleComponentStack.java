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

package org.apache.bookkeeper.common.component;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;

/**
 * A stack of {@link LifecycleComponent}s.
 */
public class LifecycleComponentStack implements LifecycleComponent {

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder to build a stack of {@link LifecycleComponent}s.
     */
    public static class Builder {

        private String name;
        private final List<LifecycleComponent> components;

        private Builder() {
            components = Lists.newArrayList();
        }

        public Builder addComponent(LifecycleComponent component) {
            checkNotNull(component, "Lifecycle component is null");
            components.add(component);
            return this;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public LifecycleComponentStack build() {
            checkNotNull(name, "Lifecycle component stack name is not provided");
            checkArgument(!components.isEmpty(), "Lifecycle component stack is empty : " + components);
            return new LifecycleComponentStack(
                name,
                ImmutableList.copyOf(components));
        }

    }

    private final String name;
    private final ImmutableList<LifecycleComponent> components;

    private LifecycleComponentStack(String name,
                                    ImmutableList<LifecycleComponent> components) {
        this.name = name;
        this.components = components;
    }

    @VisibleForTesting
    public int getNumComponents() {
        return components.size();
    }

    @VisibleForTesting
    public LifecycleComponent getComponent(int index) {
        return components.get(index);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Lifecycle.State lifecycleState() {
        return components.get(0).lifecycleState();
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {
        components.forEach(component -> component.addLifecycleListener(listener));
    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {
        components.forEach(component -> component.removeLifecycleListener(listener));
    }

    @Override
    public void start() {
        components.forEach(component -> component.start());
    }

    @Override
    public void stop() {
        components.reverse().forEach(component -> component.stop());
    }

    @Override
    public void close() {
        components.reverse().forEach(component -> component.close());
    }

    @Override
    public void setExceptionHandler(UncaughtExceptionHandler handler) {
        components.forEach(component -> component.setExceptionHandler(handler));
    }
}
