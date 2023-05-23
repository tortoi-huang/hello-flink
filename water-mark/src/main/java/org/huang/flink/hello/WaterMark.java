package org.huang.flink.hello;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class WaterMark {
    private static final BlockingQueue<Tuple2<String, Long>> queue = new LinkedBlockingQueue<>();

    public static void main(String[] args) {

        try {
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            //env.setStateBackend(new FsStateBackend("file1:///study/flink/data/checkpoints"));
            env.getConfig().setAutoWatermarkInterval(500);//定时生成Watermark的时间间隔
            DataStream<Tuple2<String, Long>> text = env.addSource(new MySourceFunction())
                    .assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple2<String, Long>>() {
                        @Override
                        public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                            return new WatermarkGenerator<Tuple2<String, Long>>() {
                                private long time = 0;

                                @Override
                                public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
                                    time = event.f1 > time ? event.f1 : time;
                                    System.out.println("-- process time: " + time);
                                }

                                @Override
                                public void onPeriodicEmit(WatermarkOutput output) {
                                    long wt = time - 5000;
                                    output.emitWatermark(new Watermark(wt));
                                    System.out.println("-- process time: " + System.currentTimeMillis() + ", emitWatermark: " + wt);
                                }
                            };
                        }

                        @Override
                        public TimestampAssigner<Tuple2<String, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                            return new TimestampAssigner<Tuple2<String, Long>>() {
                                @Override
                                public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                    return element.f1;
                                }
                            };
                        }
                    })

                    /*.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                            .withTimestampAssigner(
                                    (TimestampAssignerSupplier<Tuple2<String, Long>>) context -> (element, recordTimestamp) -> element.f1))*/
                    ;
            text.map(t -> Tuple2.of(t.f0, 1)).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(t -> t.f0)
                    //.timeWindow(Time.seconds(10), Time.seconds(5))
                    .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))

                    .sum(1)
                    .print();

            env.execute("--Watermarks test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 保证开始时间在整10秒启动，如10秒、20秒，不能在18秒启动
     */
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
        private boolean isRunning = false;
        @Override
        public void run(SourceContext<Tuple2<String, Long>> ctx) {
            isRunning = true;
            waitTo10();
            long baseTime = System.currentTimeMillis();
            try {
                System.out.println("--client push data start");
                ctx.collect(Tuple2.of("hello", 3000L));   //窗口[-5000,4999],[0,9999]
                ctx.collect(Tuple2.of("hello", 4999L));   //窗口[-5000,4999],[0,9999]
                ctx.collect(Tuple2.of("hello", 5000L));   //窗口[0,9999],[5000,14999]
                ctx.collect(Tuple2.of("hello", 7000L));   //窗口[0,9999],[5000,14999]
                ctx.collect(Tuple2.of("hello", 9000L));   //窗口[0,9999],[5000,14999]
                ctx.collect(Tuple2.of("hello", 13000L));  //窗口[5000,14999],[10000,19999]
                ctx.collect(Tuple2.of("hello", 17000L));  //窗口[10000,19999],[15000,24999]
                ctx.collect(Tuple2.of("hello", 20000L));  //窗口[15000,24999],[20000,29999] //更新水位线时间为 20000L - 5000
                Thread.sleep(1000);  //这里暂停了1秒，等待水位线每0.5秒生成了2个水位线，让窗口[5000,14999]关闭
                System.out.println("--client push delayed data: " + ( System.currentTimeMillis() - baseTime));
                ctx.collect(Tuple2.of("hello", 12000L));  //窗口[10000,19999]
                ctx.collect(Tuple2.of("hello", 20000L));  //窗口[15000,24999],[20000,29999]

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            isRunning = false;
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}
