package org.huang.flink.graph2;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.huang.flink.common.TypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Graph2 {
    private static final Logger LOG = LoggerFactory.getLogger(Graph2.class);
    public static void main(String[] args) throws Exception {
        LOG.info("===========App starting");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("127.0.0.1",8081, "entrypoint/appdemo/build/libs/appdemo-1.0.0-SNAPSHOT.jar");
        final String s = "hello flink, i like flink, flink is a even driven stream process framework";
        LOG.info("=========== param: {}", s);
        DataStreamSource<String> source = env.fromElements(s);
        source
                .flatMap((e, c) -> {
                    String[] split = e.split("[\\s,.]");
                    for (int i = 0; i < split.length; i++) {
                        c.collect(Tuple2.of(split[i], i + 5000));
                    }
                }, TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
                .assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple2<String, Integer>>() {
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
                })
                .map(value -> Tuple2.of(value.f0, 1),
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
                .keyBy(e -> e.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(30)))
                .reduce((e, c) -> {
                    c.f1 = c.f1 + e.f1;
                    return c;
                })
                .print();
        //env.execute();
        executeEnv(env);
    }

    private static void executeEnv(StreamExecutionEnvironment env) throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(DeploymentOptions.TARGET, "local");
        configuration.set(DeploymentOptions.ATTACHED, true);
        configuration.set(DeploymentOptions.SHUTDOWN_IF_ATTACHED, false);
        configuration.set(SavepointConfigOptions.RESTORE_MODE, RestoreMode.NO_CLAIM);
        configuration.set(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, false);

        StreamGraph streamGraph = env.getStreamGraph(true);

        //查看执行计划
        LOG.info(streamGraph.getStreamingPlanAsJSON());

        DefaultExecutorServiceLoader executorServiceLoader = new DefaultExecutorServiceLoader();
        final PipelineExecutorFactory executorFactory = executorServiceLoader.getExecutorFactory(configuration);
        final PipelineExecutor executor = executorFactory.getExecutor(configuration);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        executor.execute(streamGraph, configuration, classLoader);
        //JobGraph jobGraph = streamGraph.getJobGraph(classLoader, null);
    }
}
