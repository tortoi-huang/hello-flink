package org.huang.flink.entrypoint.appdemo;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        LOG.info("===========App starting");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("127.0.0.1",8081, "entrypoint/appdemo/build/libs/appdemo-1.0.0-SNAPSHOT.jar");
        final String s = "=========== hello flink, i like flink, flink is a even driven stream process framework";
        LOG.info("=========== param: {}", s);
        DataStreamSource<String> source = env.fromElements(s);
        source
                /*.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String e, Collector<Tuple2<String, Integer>> c) throws Exception {
                        String[] split = e.split("[\\s,.]");
                        for (int i = 0; i < split.length; i++) {
                            c.collect(Tuple2.of(split[i], i + 5000));
                        }
                    }
                })*/
                .flatMap(new Fm())
                /*.assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple2<String, Integer>>() {
                    @Override
                    public WatermarkGenerator<Tuple2<String, Integer>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<Tuple2<String, Integer>>() {
                            private long time = 0;

                            @Override
                            public void onEvent(Tuple2<String, Integer> event, long eventTimestamp, WatermarkOutput output) {
                                time = event.f1 > time ? event.f1 : time;
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {
                                output.emitWatermark(new Watermark(time));
                            }
                        };
                    }

                    @Override
                    public TimestampAssigner<Tuple2<String, Integer>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                        return (e, t) -> e.f1;
                    }
                })*/
                .assignTimestampsAndWatermarks(new Ws())
                /*.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                        return Tuple2.of(value.f0, 1);
                    }
                })*/
                .map(new Mf())
                //.keyBy(e -> e.f0)
                .keyBy(new Ks())
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(30)))
                /*.reduce((e, c) -> {
                    c.f1 = c.f1 + e.f1;
                    return c;
                })*/
                .reduce(new Rs())
                .print();
        env.execute();
    }
}
