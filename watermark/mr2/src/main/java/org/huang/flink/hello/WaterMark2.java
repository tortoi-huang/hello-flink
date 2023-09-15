package org.huang.flink.hello;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 学习和了解底层api操作
 */
public class WaterMark2 {
	private static final BlockingQueue<Tuple3<String, Long,String>> queue = new LinkedBlockingQueue<>();
	private static volatile long baseTime = 0;

	public static void main(String[] args) {

		try {
			final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			DataStream<Tuple3<String, Long,String>> text = env.addSource(new MySourceFunction())
					.assignTimestampsAndWatermarks(new TimestampExtractor())
					;
			text.keyBy(e -> e.f0)
					.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
					.process(new MySum())
					.print();

			provide();
			env.execute("Socket Window WordCount");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static class MySum extends ProcessWindowFunction<Tuple3<String, Long,String>, Tuple2<String, Long>, String, TimeWindow> {
		@Override
		public void process(String key, Context context, Iterable<Tuple3<String, Long,String>> elements,
				Collector<Tuple2<String, Long>> out) {
			long count = 0;
			String newKey = key + ",";
			for(Tuple3<String, Long,String> e : elements) {
				newKey = newKey + e.f2 + ",";
				count ++;
			}
			Tuple2<Long, Long> wd = Tuple2.of(rebaseTime(context.window().getStart()), rebaseTime(context.window().getEnd()));
			newKey = newKey + wd + ", currentWatermark:" + rebaseTime(context.currentWatermark()) + ",currentProcessingTime" + rebaseTime(context.currentProcessingTime());
			out.collect(Tuple2.of(newKey,count));
		}
	}

	private static long rebaseTime(long time) {
		return (time - baseTime) / 1000;
	}

	private static void provide() {
		new Thread(() -> {
			try {
				//等待 flink启动
				System.out.println("等待 flink启动");
				Thread.sleep(10000);
				System.out.println("等待 下一个整10秒");
				waitTo10();
				baseTime = System.currentTimeMillis();
				System.out.println("推送数据了0");
				queue.offer(Tuple3.of("hello", baseTime + 13000,"0-13"));
				queue.offer(Tuple3.of("hello", baseTime + 17000,"0-17"));
				Thread.sleep(18000);
				System.out.println("推送延期数据1: " + rebaseTime(System.currentTimeMillis()));
				queue.offer(Tuple3.of("hello", baseTime + 12000,"18-12"));
				Thread.sleep(3000);
				System.out.println("推送延期数据2: " + rebaseTime(System.currentTimeMillis()));
				//这个数据丢失了，因为超出水印时间超过5秒
				queue.offer(Tuple3.of("hello", baseTime + 5000,"21-5"));
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

	static class MySourceFunction implements SourceFunction<Tuple3<String, Long,String>> {
		private volatile boolean isRunning = true;
		@Override
		public void run(SourceContext<Tuple3<String, Long,String>> ctx) {
			while (isRunning) {
				try {
					final Tuple3<String, Long,String> poll = queue.take();
					System.out.println("poll:" + poll.f2);
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

	static class TimestampExtractor implements WatermarkStrategy<Tuple3<String, Long,String>> {
		@Override
		public WatermarkGenerator<Tuple3<String, Long, String>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
			return new WatermarkGenerator<Tuple3<String, Long, String>>() {
				private long time = 0;

				@Override
				public void onEvent(Tuple3<String, Long, String> event, long eventTimestamp, WatermarkOutput output) {
					time = event.f1 > time ? event.f1 : time;
				}

				@Override
				public void onPeriodicEmit(WatermarkOutput output) {
					long wt = time - 5000;
					output.emitWatermark(new Watermark(wt));
				}
			};
		}

		@Override
		public TimestampAssigner<Tuple3<String, Long, String>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
			return (e,t) -> e.f1;
		}
	}
}
