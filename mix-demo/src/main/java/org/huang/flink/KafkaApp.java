package org.huang.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class KafkaApp {
    public static void main(String[] args) throws Exception {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:19092,localhost:19093,localhost:19094")
                .setTopics("input-topic")
                .setGroupId("my-group1")
                // 根据kafka消费组状态，消费，如果没有查到消费组状态则从最早的记录开始消费
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                // 不管消费组状态，总是从最早的开始消费
                // .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("commit.offsets.on.checkpoint","true")
                .build();

        Configuration configuration = new Configuration();
        configuration.set(StateBackendOptions.STATE_BACKEND, "filesystem");
        configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///D:/tmp/kafka_app-checkpoint");
        configuration.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, "file:///D:/tmp/kafka_app-savepoint");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        // enable checkpoint
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka Source")
                // set 4 parallel source tasks
                .setParallelism(4)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering
        env.execute("kafka steam");

    }
}
