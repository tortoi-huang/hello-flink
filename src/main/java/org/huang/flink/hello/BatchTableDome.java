package org.huang.flink.hello;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

public class BatchTableDome {
	static String file1 = "E:\\project\\study\\huang-lab\\python\\webdb\\export_by_ids\\export_by_ids3_1.csv";
	static TypeInformation stringType = TypeInformation.of(String.class);
	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		final CsvTableSource.Builder builder = new CsvTableSource.Builder();
		builder.path(file1)
				.field("order_no",stringType)
				.field("cust_order_no",stringType)
				.field("status",TypeInformation.of(Byte.class))
				.field("time1",stringType)
				.field("time2",stringType)
				.field("time3",stringType);
		BatchTableEnvironment environment = BatchTableEnvironment.create(env);
		environment.registerTableSource("orders",builder.build());

		CsvTableSink sink = new CsvTableSink("E:\\project\\tmp\\output\\flink_sql",",",1, FileSystem.WriteMode.OVERWRITE);
		final TableSink<Row> configure = sink.configure(new String[] { "status", "count_od" },
				new TypeInformation[] { TypeInformation.of(Byte.class), TypeInformation.of(Long.class) });
		environment.registerTableSink("order_group",configure);
		final long start = System.nanoTime();
		final Table table = environment.sqlQuery("select status,count(1) from orders group by status order by status");

		System.out.println(environment.explain(table));

		table.insertInto("order_group");
		//environment.toDataSet(table, Row.class).print();
		env.execute();

		System.out.println("excute time:" + (System.nanoTime() - start) / 1_000_000);
	}
}
