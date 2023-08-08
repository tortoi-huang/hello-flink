package org.huang.flink.graph;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.huang.flink.common.*;

import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 */
public class Graph {
    public static void main(String[] args) throws Exception {

        //StreamExecutionEnvironment和ExecutionEnvironment中有三个线程变量是从submit时传过来的: executorServiceLoader/configuration/userCodeClassLoader
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("127.0.0.1",8081, "D:\\study\\flink\\hello-flink\\graph\\build\\libs\\graph-1.0.0-SNAPSHOT.jar");

        //TODO source不会往env添加Transformation，流执行的时候如何调用source function？ 已知生成stream graph 时执行StreamGraphGenerator.transform转换下列第一个算子时计算出来
        DataStreamSource<String> source = env.addSource(new FlinkGraphSource(), "FG source");
        System.out.println("-- stream transformation, source: " + source.getTransformation());
        System.out.println();

        //往StreamExecutionEnvironment.transformations添加一个元素
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapped = source.map(new StringAndOneTuple2Map()).name("FG mapped");
        System.out.println("-- stream transformation, mapped: " + mapped.getTransformation());
        System.out.println("-- env transformations, mapped: " + env.getTransformations());
        System.out.println();

        KeyedStream<Tuple2<String, Integer>, String> keyed = mapped.keyBy(new Tuple2Feild1KeySelector<>());
        System.out.println("-- stream transformation, keyed: " + keyed.getTransformation());
        System.out.println("-- env transformations, keyed: " + env.getTransformations());
        System.out.println();

        //WindowedStream 不是DataStream 没有Transformation
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window =
                keyed.window(SlidingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS), Time.of(5, TimeUnit.SECONDS)));
        System.out.println("-- env transformations, window: " + env.getTransformations());
        System.out.println();

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduced =
                window.reduce(new SumTuple2F1ReduceFunction()).name("FG reduced");
        System.out.println("-- stream transformation, reduced: " + reduced.getTransformation());
        System.out.println("-- env transformations, reduce: " + env.getTransformations());
        System.out.println();

        DataStreamSink<Tuple2<String, Integer>> printed = reduced.addSink(new SystemOutSinkFunction<>()).name("FG printed");
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
        JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(streamGraph, (Configuration) env.getConfiguration(), Graph.class.getClassLoader());
        System.out.println("-- job graph: " + jobGraph);
        jobGraph.getVertices().forEach(e -> {
            System.out.println("------ 顶点信息 task Vertic: " + e);
        });

        System.out.println(env.getExecutionPlan());
        System.out.println("--------------------------------------- execute ---------------------------------------");
        JobExecutionResult executionResult = env.execute("flink graph");
        System.out.println("-- JobExecutionResult: " + executionResult);

        //ExecutionEnvironment env2 = ExecutionEnvironment.getExecutionEnvironment();
    }
}
