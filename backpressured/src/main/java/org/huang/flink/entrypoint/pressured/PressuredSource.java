package org.huang.flink.entrypoint.pressured;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class PressuredSource implements SourceFunction<String> {
    final String[] sourceBase = new String[]{
            "Besides the obvious use of each individual metric mentioned above, there are also a few combinations providing useful insight into what is happening in the network stack",
            "Besides the obvious use of each individual metric mentioned above, there are also a few combinations providing useful insight into what is happening in the network stack",
            "Besides the obvious use of each individual metric mentioned above, there are also a few combinations providing useful insight into what is happening in the network stack",
            "Besides the obvious use of each individual metric mentioned above, there are also a few combinations providing useful insight into what is happening in the network stack",
            "If all subtasks of the receiver task have low inPoolUsage values and any upstream subtask’s outPoolUsage is high, then there may be a network bottleneck causing backpressure. Since network is a shared resource among all subtasks of a TaskManager, this may not directly originate from this subtask, but rather from various concurrent operations, e.g. checkpoints, other streams, external connections, or other TaskManagers/processes on the same machine",
            "Combining numRecordsOut and numBytesOut helps identifying average serialised record sizes which supports you in capacity planning for peak scenarios",
            "",
            "Try to optimise your code. Code profilers are helpful in this case.",
            "Tune Flink for that specific resource",
            "Scale out by increasing the parallelism and/or increasing the number of machines in the cluster.",
            "Similarly to the CPU/thread bottleneck issue above, a subtask may be bottlenecked due to high thread contention on shared resources. Again, CPU profilers are your best friend here! Consider looking for synchronisation overhead / lock contention in user code — although adding synchronisation in user code should be avoided and may even be dangerous! Also consider investigating shared system resources. The default JVM’s SSL implementation, for example, can become contented around the shared /dev/urandom resource.",
            "Similarly to the CPU/thread bottleneck issue above, a subtask may be bottlenecked due to high thread contention on shared resources. Again, CPU profilers are your best friend here! Consider looking for synchronisation overhead / lock contention in user code — although adding synchronisation in user code should be avoided and may even be dangerous! Also consider investigating shared system resources. The default JVM’s SSL implementation, for example, can become contented around the shared /dev/urandom resource.",
            "Unless required by applicable law or agreed to in writing, software",
            "Tracking latencies at the various locations they may occur is a topic of its own. In this section, we will focus on the time records wait inside Flink’s network stack — including the system’s network connections. In low throughput scenarios, these latencies are influenced directly by the output flusher via the buffer timeout parameter or indirectly by any application code latencies. When processing a record takes longer than expected or when (multiple) timers fire at the same time — and block the receiver from processing incoming records — the time inside the network stack for following records is extended dramatically. We highly recommend adding your own metrics to your Flink job for better latency tracking in your job’s components and a broader view on the cause of delays",
            "end"
    };
    final Random sourceIndex = new Random();

    final int sleepTime;

    private volatile boolean cancel = false;

    public PressuredSource(int sleepTime) {
        this.sleepTime = sleepTime;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        long timestamp = 0;
        while (!cancel) {
            try {
                String even = sourceBase[sourceIndex.nextInt(sourceBase.length)];
                ctx.collectWithTimestamp(even, timestamp);
                int time = sourceIndex.nextInt(sleepTime);
                Thread.sleep(time);
                timestamp += time;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void cancel() {
        cancel = true;
    }
}
