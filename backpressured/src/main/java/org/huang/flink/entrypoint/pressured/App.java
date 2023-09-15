package org.huang.flink.entrypoint.pressured;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        LOG.info("===========App starting");
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("127.0.0.1",8081, "backpressured/build/libs/backpressured-1.0.0-SNAPSHOT.jar");
        env.setParallelism(2);

        SingleOutputStreamOperator<String> source = env.addSource(new PressuredSource(10)).name("PressuredSource");
        source.assignTimestampsAndWatermarks(new Ws()).name("Ws WatermarkStrategy")
                .flatMap(new Fm()).name("Fm flatMap")
                .map(new Mf()).name("Mf map")
                .keyBy(new Ks())
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .reduce(new Rs()).name("Rs reduce")
                .print().name("print sink");
        env.execute();
    }
}
