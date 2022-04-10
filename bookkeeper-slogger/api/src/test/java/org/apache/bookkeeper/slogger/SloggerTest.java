/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.slogger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.Test;

/**
 * Test Slogger.
 */
public class SloggerTest {
    enum Events {
        FOOBAR,
        BARFOO
    };

    @Test
    public void testBasic() throws Exception {
        MockSlogger root = new MockSlogger();
        root.kv("foo", 2324).kv("bar", 2342).info(Events.FOOBAR);
        assertThat(root.events, hasSize(1));
        assertThat(root.events.get(0).getLevel(), is(MockSlogger.Level.INFO));
        assertThat(root.events.get(0).getEvent(), is(Events.FOOBAR));
        assertThat(root.events.get(0).getKeyValues(),
                   allOf(hasEntry("foo", "2324"),
                         hasEntry("bar", "2342")));
    }

    @Test
    public void testSloggable() throws Exception {
        MockSlogger root = new MockSlogger();
        root.kv("fancy", new FancyClass(0, 2)).info(Events.FOOBAR);
        assertThat(root.events, hasSize(1));
        assertThat(root.events.get(0).getLevel(), is(MockSlogger.Level.INFO));
        assertThat(root.events.get(0).getEvent(), is(Events.FOOBAR));
        assertThat(root.events.get(0).getKeyValues(),
                   allOf(hasEntry("fancy.foo", "0"),
                         hasEntry("fancy.bar", "2"),
                         hasEntry("fancy.baz.baz", "123")));
    }

    @Test
    public void testList() throws Exception {
        MockSlogger root = new MockSlogger();
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        root.kv("list", list).info(Events.FOOBAR);

        assertThat(root.events, hasSize(1));
        assertThat(root.events.get(0).getLevel(), is(MockSlogger.Level.INFO));
        assertThat(root.events.get(0).getEvent(), is(Events.FOOBAR));
        assertThat(root.events.get(0).getKeyValues(), hasEntry("list", "[1, 2]"));
    }

    @Test
    public void testMap() throws Exception {
        MockSlogger root = new MockSlogger();
        HashMap<Integer, Integer> map = new HashMap<>();
        map.put(1, 3);
        map.put(2, 4);
        root.kv("map", map).info(Events.FOOBAR);

        assertThat(root.events, hasSize(1));
        assertThat(root.events.get(0).getLevel(), is(MockSlogger.Level.INFO));
        assertThat(root.events.get(0).getEvent(), is(Events.FOOBAR));
        assertThat(root.events.get(0).getKeyValues(), hasEntry("map", "{1=3, 2=4}"));
    }

    @Test
    public void testArray() throws Exception {
        MockSlogger root = new MockSlogger();
        String[] array = {"foo", "bar"};
        root.kv("array", array).info(Events.FOOBAR);

        assertThat(root.events, hasSize(1));
        assertThat(root.events.get(0).getLevel(), is(MockSlogger.Level.INFO));
        assertThat(root.events.get(0).getEvent(), is(Events.FOOBAR));
        assertThat(root.events.get(0).getKeyValues(), hasEntry("array", "[foo, bar]"));
    }

    @Test
    public void testNestingLimit() throws Exception {
    }

    @Test
    public void testCtx() throws Exception {
        MockSlogger root = new MockSlogger();
        MockSlogger withCtx = (MockSlogger) root.kv("ctx1", 1234).kv("ctx2", 4321).ctx();

        withCtx.kv("someMore", 2345).info(Events.FOOBAR);

        assertThat(withCtx.events, hasSize(1));
        assertThat(withCtx.events.get(0).getLevel(), is(MockSlogger.Level.INFO));
        assertThat(withCtx.events.get(0).getEvent(), is(Events.FOOBAR));
        System.out.println("kvs " +  withCtx.events.get(0).getKeyValues());
        assertThat(withCtx.events.get(0).getKeyValues(),
                   allOf(hasEntry("ctx1", "1234"),
                         hasEntry("ctx2", "4321"),
                         hasEntry("someMore", "2345")));
    }

    @Test
    public void textCtxImmutableAfterCreation() throws Exception {
    }

    static class FancyClass implements Sloggable {
        int foo;
        int bar;
        OtherFancyClass baz;

        FancyClass(int foo, int bar) {
            this.foo = foo;
            this.bar = bar;
            this.baz = new OtherFancyClass(123);
        }

        @Override
        public SloggableAccumulator log(SloggableAccumulator slogger) {
            return slogger.kv("foo", foo)
                .kv("bar", bar)
                .kv("baz", baz);
        }
    }

    static class OtherFancyClass implements Sloggable {
        int baz;

        OtherFancyClass(int baz) {
            this.baz = baz;
        }

        @Override
        public SloggableAccumulator log(SloggableAccumulator slogger) {
            return slogger.kv("baz", baz);
        }
    }
}
