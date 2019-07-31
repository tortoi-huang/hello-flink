package org.huang.flink.hello;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class WaterMark {
	private static final BlockingQueue<Tuple2<String, Long>> queue = new LinkedBlockingQueue<>();

	public static void main(String[] args) {

		try {
			final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.setStateBackend((StateBackend)new FsStateBackend("file:///study/flink/data/checkpoints"));
			DataStream<Tuple2<String, Long>> text = env.addSource(new MySourceFunction())
					.assignTimestampsAndWatermarks(new TimestampExtractor())
					;
			text.map(t -> Tuple2.of(t.f0, 1)).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(0)
					.timeWindow(Time.seconds(10), Time.seconds(5)).sum(1).print();

			provide();
			env.execute("Socket Window WordCount");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void provide() {
		new Thread(() -> {
			try {
				//等待 flink启动
				System.out.println("等待 flink启动");
				Thread.sleep(10000);
				System.out.println("等待 下一个整10秒");
				waitTo10();
				long baseTime = System.currentTimeMillis();
				System.out.println("推送数据了" + (baseTime / 1000));
				queue.offer(Tuple2.of("hello", baseTime + 13000));
				queue.offer(Tuple2.of("hello", baseTime + 17000));
				Thread.sleep(18000);
				System.out.println("推送延期数据");
				queue.offer(Tuple2.of("hello", baseTime + 12000));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}).start();
	}

	private static void waitTo10() {
		long errTime = 500;
		for (; ; ) {
			if (System.currentTimeMillis() % 10000 <= errTime) {
				break;
			}
			try {
				Thread.sleep(errTime / 2);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	static class MySourceFunction implements SourceFunction<Tuple2<String, Long>> {
		private volatile boolean isRunning = true;
		@Override
		public void run(SourceContext<Tuple2<String, Long>> ctx) {
			while (isRunning) {
				try {
					final Tuple2<String, Long> poll = queue.take();
					System.out.println("poll:" + poll);
					if (poll != null) {
						ctx.collectWithTimestamp(poll,poll.f1);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	static class TimestampExtractor implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {
		@Nullable
		@Override
		public Watermark getCurrentWatermark() {
			long waterTimes = System.currentTimeMillis() - 5000;
			return new Watermark(waterTimes);
		}

		@Override
		public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
			return element.f1;
		}
	}
}
