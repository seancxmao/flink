package org.apache.flink.streaming.examples.windowing.rocksdb;

import org.apache.flink.contrib.streaming.state.DefaultConfigurableOptionsFactory;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.ZonedDateTime;
import java.util.UUID;

public class RocksDBSessionWindowing {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(300000L, CheckpointingMode.EXACTLY_ONCE);

    RocksDBStateBackend stateBackend = new RocksDBStateBackend("file:///tmp/checkpoints", true);
    stateBackend.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED);
    DefaultConfigurableOptionsFactory optionsFactory = new DefaultConfigurableOptionsFactory();
    optionsFactory.setBlockCacheSize("4096m");
    stateBackend.setRocksDBOptions(optionsFactory);
    env.setStateBackend(stateBackend);

    DataStreamSource<TestEvent> dataStreamSource = env.addSource(new SourceFunction<TestEvent>() {
      @Override
      public void run(SourceContext<TestEvent> ctx) throws Exception {
        for (int i = 0; i < 5000; i++) {
          TestEvent event = new TestEvent();
          event.setGuid(UUID.randomUUID().toString());
          event.setTimestamp(ZonedDateTime.now().toEpochSecond());
          event.setData(new byte[1024 * 1024]);
          ctx.collectWithTimestamp(event, event.getTimestamp());
          ctx.emitWatermark(new Watermark(event.getTimestamp() - 1));
        }
        ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
      }

      @Override
      public void cancel() {
      }
    }).setParallelism(1);

    SingleOutputStreamOperator<TestSession> sessionDataStream =
        dataStreamSource
            .keyBy("guid")
            .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
            .allowedLateness(Time.minutes(3))
            .aggregate(new SessionAgg())
            .name("Sessionizer")
            .setParallelism(1);

    sessionDataStream.addSink(new DiscardingSink<>())
        .name("Sink")
        .setParallelism(1);

    env.execute("RocksDBSessionWindowing");
    while (true) {
    	Thread.sleep(60 * 1000);
	}
  }
}
