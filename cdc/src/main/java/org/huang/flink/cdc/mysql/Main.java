package org.huang.flink.cdc.mysql;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.*;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * 首次启动会读取表快照，
 * 后续读取会送之前的binlog处重新开始
 */
public class Main {
    public static void main(String[] args) throws Exception {
        String host = getEnv("MARIADB_HOST", "127.0.0.1");
        int port = Integer.parseInt(getEnv("MARIADB_PORT", "3306"));
        String user = getEnv("MARIADB_USER", "root");
        String password = getEnv("MARIADB_PASSWORD", "Mypass@112233");
        String database = getEnv("MARIADB_DATABASE", "test");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(host)
                .port(port)
                .databaseList(database) // set captured database
                .tableList("test.person") // set captured table
                .username(user)
                .password(password).serverTimeZone("UTC")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .scanNewlyAddedTableEnabled(true) //新增表也支持同步
//                .startupOptions(StartupOptions.latest())
                .build();

        //每次重启总是会读取数据库快照，怎么设置都没用
        Configuration configuration = new Configuration();
        configuration.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        configuration.set(CheckpointingOptions. CHECKPOINT_STORAGE, "filesystem");
        String checkpointPath = mkdirInUserHome("/fl-checkpoint");
        configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointPath);
        configuration.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, mkdirInUserHome("/fl-savepoint"));

        // 这里需要固定 job id 否则每次重启时ID会变，无法读取上次停机时的checkpoints, 需要手动指定checkpoints
        configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID,"b7a2c8ba2d80390c4ee5e288fe93a994");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // enable checkpoint
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source").uid("mySqlSource")
                // set 4 parallel source tasks
                .setParallelism(4)
                .print().uid("print").setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
    }

    private static String mkdirInUserHome(String path) {
        String stateDir = getEnv("STATE_DIR", System.getProperty("user.home") + "/temp");
        String cpDir = stateDir + path;
        File cpFile = new File(cpDir);
        if(!cpFile.exists()) {
            cpFile.mkdirs();
        }
        return "file://" + cpFile.getAbsolutePath();
    }

    private static String getEnv(String key, String defaultVal) {
        String val = System.getenv(key);
        if(val == null || val.isEmpty())
            return defaultVal;
        return val;
    }
}
