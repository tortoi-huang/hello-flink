package org.huang.flink.hello;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.io.File;
import java.io.FileNotFoundException;

public class BatchTableDome {
    static final String SOURCE_FILE = "batch-table/build/resources/main/source/exeample.csv";
    static final String SINK_FILE = "batch-table/build/resources/main/sink/";
    static TypeInformation stringType = TypeInformation.of(String.class);

    public static void main(String[] args) throws Exception {

        File sourceFile = new File(SOURCE_FILE);
        if (!sourceFile.exists()) {
            throw new FileNotFoundException("File not found:" + sourceFile.getAbsolutePath());
        }
        File sinkFile = new File(SINK_FILE);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                //.inStreamingMode()//使用流模式则所有的查询只能根据时间戳排序
                .inBatchMode()
                .build();
        TableEnvironment environment = TableEnvironment.create(settings);

        environment.executeSql("create table orders (" +
                "order_no STRING, cust_order_no STRING, status TINYINT, time1 TIMESTAMP) " +
                "with (" +
                "'connector' = 'filesystem'," +
                "'format' = 'csv'," +
                "'path' = 'file:///" + sourceFile.getAbsolutePath() + "'" +
                ")");

        environment.executeSql("create table status_count (" +
                "status TINYINT,CNT BIGINT) " +
                "with (" +
                "'connector' = 'filesystem'," +
                "'format' = 'csv'," +
                "'path' = 'file:///" + sinkFile.getAbsolutePath() + "'" +
                ")");

        environment
                .executeSql("insert into status_count select status,count(1) cnt from orders group by status order by status")
                .print();
    }
}
