/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.distributedlog.stream.client.impl.view.kv;

import static org.apache.distributedlog.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.router.HashRouter;
import org.apache.bookkeeper.common.util.ByteBufUtils;
import org.apache.bookkeeper.common.util.Bytes;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.distributedlog.stream.api.StreamConfig;
import org.apache.distributedlog.stream.api.view.kv.Table;
import org.apache.distributedlog.stream.api.view.kv.options.DeleteOption;
import org.apache.distributedlog.stream.api.view.kv.options.GetOption;
import org.apache.distributedlog.stream.api.view.kv.options.PutOption;
import org.apache.distributedlog.stream.client.exceptions.ClientException;
import org.apache.distributedlog.stream.client.impl.RangeRouter;
import org.apache.distributedlog.stream.client.internal.api.HashStreamRanges;
import org.apache.distributedlog.stream.client.internal.api.MetaRangeClient;
import org.apache.distributedlog.stream.client.internal.api.RangeServerClientManager;
import org.apache.distributedlog.stream.proto.RangeKeyType;
import org.apache.distributedlog.stream.proto.RangeProperties;
import org.apache.distributedlog.stream.proto.StreamProperties;
import org.apache.distributedlog.stream.protocol.util.ProtoUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test for {@link TableImpl}.
 */
public class TestTableImpl {

  private static final long streamId = 12345L;

  @Rule
  public TestName runtime = new TestName();

  private final HashStreamRanges streamRanges1 = prepareRanges(streamId, 4, 0);
  private final HashStreamRanges streamRanges2 = prepareRanges(streamId, 8, 4L);
  private final HashStreamRanges streamRanges3 = prepareRanges(streamId, 80, 12L);
  private final HashRouter<Integer> router = new HashRouter<Integer>() {

    private static final long serialVersionUID = -9119055960554608491L;

    private final List<Long> keys = Lists.newArrayList(streamRanges3.getRanges().keySet());

    @Override
    public Long getRoutingKey(Integer key) {
      int idx;
      if (null == key) {
        idx = ThreadLocalRandom.current().nextInt(keys.size());
      } else {
        idx = key % keys.size();
      }
      return keys.get(idx);
    }
  };
  private final StreamProperties streamProps = StreamProperties.newBuilder()
    .setStorageContainerId(12345L)
    .setStreamConf(DEFAULT_STREAM_CONF)
    .setStreamId(streamId)
    .setStreamName("test-stream")
    .build();
  private final MetaRangeClient mockMetaRangeClient = mock(MetaRangeClient.class);
  private final RangeServerClientManager mockClientManager = mock(RangeServerClientManager.class);

  private final StreamConfig<Integer, String> streamConfig = StreamConfig.newBuilder()
    .hashRouter(router)
    .build();
  private OrderedScheduler scheduler;

  private static HashStreamRanges prepareRanges(long streamId, int numRanges, long nextRangeId) {
    List<RangeProperties> ranges = ProtoUtils.split(streamId, numRanges, nextRangeId, (sid, rid) -> 1L);
    NavigableMap<Long, RangeProperties> rangeMap = Maps.newTreeMap();
    for (RangeProperties props : ranges) {
      rangeMap.put(props.getStartHashKey(), props);
    }
    return HashStreamRanges.ofHash(
      RangeKeyType.HASH,
      rangeMap);
  }

  @Before
  public void setUp() {
    when(mockClientManager.openMetaRangeClient(any(StreamProperties.class)))
      .thenReturn(mockMetaRangeClient);
    scheduler = OrderedScheduler.newSchedulerBuilder()
      .numThreads(1)
      .name("test-scheduler")
      .build();
  }

  @Test
  public void testInitializeFailureOnGetActiveRanges() {
    ClientException cause = new ClientException("test-cause");
    when(mockMetaRangeClient.getActiveDataRanges())
      .thenReturn(FutureUtils.exception(cause));

    TableImpl table = new TableImpl(
      runtime.getMethodName(),
      streamProps,
      mockClientManager,
      scheduler.chooseThread(1));
    try {
      FutureUtils.result(table.initialize());
      fail("Should fail initializing the table with exception " + cause);
    } catch (Exception e) {
      assertEquals(cause, e);
    }
  }

  @Test
  public void testBasicOperations() throws Exception {
    when(mockMetaRangeClient.getActiveDataRanges())
      .thenReturn(FutureUtils.value(streamRanges1));

    ConcurrentMap<Long, Table> tableRanges = Maps.newConcurrentMap();
    for (RangeProperties rangeProps : streamRanges1.getRanges().values()) {
      tableRanges.put(rangeProps.getRangeId(), mock(Table.class));
    }

    RangeRouter<ByteBuf> mockRouter = mock(RangeRouter.class);
    when(mockRouter.getRange(any(ByteBuf.class)))
      .thenAnswer(invocationOnMock -> {
        ByteBuf key = invocationOnMock.getArgument(0);
        byte[] keyData = ByteBufUtils.getArray(key);
        return Bytes.toLong(keyData, 0);
      });

    TableRangeFactory trFactory = (streamProps1, rangeProps, executor) -> tableRanges.get(rangeProps.getRangeId());
    TableImpl table = new TableImpl(
      runtime.getMethodName(),
      streamProps,
      mockClientManager,
      scheduler.chooseThread(),
      trFactory,
      Optional.of(mockRouter));
    assertEquals(0, table.getTableRanges().size());
    verify(mockRouter, times(0)).setRanges(any(HashStreamRanges.class));

    // initialize the table
    assertTrue(table == FutureUtils.result(table.initialize()));
    verify(mockRouter, times(1)).setRanges(eq(streamRanges1));
    assertEquals(4, table.getTableRanges().size());

    // test get
    for (RangeProperties rangeProps : streamRanges1.getRanges().values()) {
      ByteBuf pkey =
        Unpooled.wrappedBuffer(Bytes.toBytes(rangeProps.getRangeId()));
      ByteBuf lkey =
        Unpooled.wrappedBuffer(Bytes.toBytes(rangeProps.getRangeId()));
      GetOption option =
        GetOption.newBuilder().build();
      table.get(pkey, lkey, option);
      verify(tableRanges.get(rangeProps.getRangeId()), times(1))
        .get(eq(pkey), eq(lkey), eq(option));
    }

    // test put
    for (RangeProperties rangeProps : streamRanges1.getRanges().values()) {
      ByteBuf pkey =
        Unpooled.wrappedBuffer(Bytes.toBytes(rangeProps.getRangeId()));
      ByteBuf lkey =
        Unpooled.wrappedBuffer(Bytes.toBytes(rangeProps.getRangeId()));
      ByteBuf value =
        Unpooled.wrappedBuffer(Bytes.toBytes(rangeProps.getRangeId()));
      PutOption option =
        PutOption.newBuilder().build();
      table.put(pkey, lkey, value, option);
      verify(tableRanges.get(rangeProps.getRangeId()), times(1))
        .put(eq(pkey), eq(lkey), eq(value), eq(option));
    }

    // test delete
    for (RangeProperties rangeProps : streamRanges1.getRanges().values()) {
      ByteBuf pkey =
        Unpooled.wrappedBuffer(Bytes.toBytes(rangeProps.getRangeId()));
      ByteBuf lkey =
        Unpooled.wrappedBuffer(Bytes.toBytes(rangeProps.getRangeId()));
      DeleteOption option =
        DeleteOption.newBuilder().build();
      table.delete(pkey, lkey, option);
      verify(tableRanges.get(rangeProps.getRangeId()), times(1))
        .delete(eq(pkey), eq(lkey), eq(option));
    }
  }
}
