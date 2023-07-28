package org.huang.flink.connect;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.fromElements("hello", "flink", "I", "like", "flink", "good", "bye")
                .keyBy(x -> x);

        DataStream<String> ctlStream = env.fromElements("flink", "bye")
                .keyBy(x -> x);

        dataStream.connect(ctlStream).flatMap(new RichCoFlatMapFunction<String, String, String>() {
            private MapState<String, Object> config;

            @Override
            public void open(Configuration parameters) throws Exception {
                //首次从持久化状态读取
                config = getRuntimeContext().getMapState(new MapStateDescriptor<>("config", String.class, Object.class));
            }

            @Override
            public void flatMap1(String value, Collector<String> out) throws Exception {
                if(config.contains(value)) {
                    out.collect(value + "!");
                } else {
                    out.collect(value);
                }

            }

            @Override
            public void flatMap2(String value, Collector<String> out) throws Exception {
                if (!config.contains(value)) {
                    config.put(value, null);
                }
            }
        }).print();
        env.execute("job1");
    }
}
