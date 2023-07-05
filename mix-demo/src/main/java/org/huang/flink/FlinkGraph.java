package org.huang.flink;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 */
public class FlinkGraph {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //TODO source不会往env添加Transformation，流执行的时候如何调用source function？ 已知生成stream graph 时执行StreamGraphGenerator.transform转换下列第一个算子时计算出来
        DataStreamSource<String> source = env.addSource(new FlinkGraphSource(), "FG source");
        System.out.println("-- stream transformation, source: " + source.getTransformation());
        System.out.println();

        //往StreamExecutionEnvironment.transformations添加一个元素
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapped = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String e) throws Exception {
                return Tuple2.of(e, 1);
            }
        }).name("FG mapped");
        System.out.println("-- stream transformation, mapped: " + mapped.getTransformation());
        System.out.println("-- env transformations, mapped: " + env.getTransformations());
        System.out.println();

        KeyedStream<Tuple2<String, Integer>, String> keyed = mapped.keyBy(e -> e.f0);
        System.out.println("-- stream transformation, keyed: " + keyed.getTransformation());
        System.out.println("-- env transformations, keyed: " + env.getTransformations());
        System.out.println();

        //WindowedStream 不是DataStream 没有Transformation
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window =
                keyed.window(SlidingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS), Time.of(5, TimeUnit.SECONDS)));
        System.out.println("-- env transformations, window: " + env.getTransformations());
        System.out.println();

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduced =
                window.reduce((k1, k2) -> Tuple2.of(k1.f0, k1.f1 + k2.f1)).name("FG reduced");
        System.out.println("-- stream transformation, reduced: " + reduced.getTransformation());
        System.out.println("-- env transformations, reduce: " + env.getTransformations());
        System.out.println();

        DataStreamSink<Tuple2<String, Integer>> printed = reduced.print().name("FG printed");
        System.out.println("-- stream transformation, printed: " + printed.getTransformation());
        System.out.println("-- env transformations, print: " + env.getTransformations());
        System.out.println();

        System.out.println("--------------------------------------- streamGraph ---------------------------------------");
        StreamGraph streamGraph = env.getStreamGraph(false);

        System.out.println("-- 流图信息stream graph: " + streamGraph.getJobName() + ", 图类型JobType" + streamGraph.getJobType()
                + ", 图的输入顶点SourceIDs" + streamGraph.getSourceIDs() + ", 图的输出顶点SinkIDs" + streamGraph.getSinkIDs());
        System.out.println("-- traverse stream graph nodes: " + streamGraph.getStreamNodes().size());
        for (StreamNode sn : streamGraph.getStreamNodes()) {
            System.out.println("---- 顶点node: " + sn + ", InEdges: " + sn.getInEdges().size() + ", OutEdges: " + sn.getOutEdges().size());
            sn.getInEdges().forEach((e) -> {
                System.out.println("------ 输入边InEdge: " + e);
            });
            sn.getOutEdges().forEach((e) -> {
                System.out.println("------ 输出边OutEdge: " + e);
            });
        }

        System.out.println("--------------------------------------- jobGraph ---------------------------------------");
        JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(streamGraph, (Configuration) env.getConfiguration(), FlinkGraph.class.getClassLoader());
        System.out.println("-- job graph: " + jobGraph);
        jobGraph.getVertices().forEach(e -> {
            System.out.println("------ 顶点信息 task Vertic: " + e);
        });

        System.out.println("--------------------------------------- execute ---------------------------------------");
        JobExecutionResult flinkGraph = env.execute("flink graph");
        System.out.println("-- JobExecutionResult: " + flinkGraph);

        //ExecutionEnvironment env2 = ExecutionEnvironment.getExecutionEnvironment();
    }

    static class FlinkGraphSource implements SourceFunction<String> {
        volatile private static boolean canceled = false;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            //while(!canceled) {
            ctx.collectWithTimestamp("hello", 1000);
            ctx.collectWithTimestamp("flink", 2000);
            ctx.collectWithTimestamp(",", 2000);
            ctx.collectWithTimestamp("I", 3000);
            ctx.collectWithTimestamp("like", 4000);
            ctx.collectWithTimestamp("flink", 5000);
            ctx.collectWithTimestamp(",", 6000);
            ctx.collectWithTimestamp("flink", 7000);
            ctx.collectWithTimestamp("is", 8000);
            ctx.collectWithTimestamp("the", 9000);
            ctx.collectWithTimestamp("best", 10000);
            ctx.collectWithTimestamp("steam", 11000);
            ctx.collectWithTimestamp("compute", 12000);
            ctx.collectWithTimestamp("framework", 13000);
            ctx.collectWithTimestamp(".", 14000);

            System.out.println("-- FlinkGraphSource sent data");
            Thread.sleep(20000);
            ctx.collectWithTimestamp("message end", 34000);
            //}
        }

        @Override
        public void cancel() {
            canceled = true;
        }
    }
}
