package org.huang.flink.cdc.mysql;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;

/**
 * 首次启动会读取表快照，
 * 后续读取会送之前的binlog处重新开始
 */
public class Main {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .databaseList("test") // set captured database
                .tableList("test.person") // set captured table
                .username("root")
                .password("Mypass@112233").serverTimeZone("UTC")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .scanNewlyAddedTableEnabled(true) //新增表也支持同步
                .build();

        Configuration configuration = new Configuration();
        //configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///D:/tmp/fl-checkpoint");
        configuration.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, "file:///D:/tmp/fl-savepoint");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // enable checkpoint
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/fl-checkpoint");

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(4)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
