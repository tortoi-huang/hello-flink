package org.huang.flink.common;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class FlinkGraphSource implements SourceFunction<String> {
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
