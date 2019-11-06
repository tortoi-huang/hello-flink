package org.huang.flink.hello;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sinks.CsvTableSink;

public class ConnectorDemo {
	static String file1 = "E:\\project\\study\\huang-lab\\python\\webdb\\export_by_ids\\export_by_ids3_1.csv";
	static TypeInformation stringType = TypeInformation.of(String.class);

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment environment = BatchTableEnvironment.create(env);

		final OldCsv csv = new OldCsv().field("order_no", stringType).field("cust_order_no", stringType)
				.field("status", TypeInformation.of(Byte.class)).field("time1", stringType).field("time2", stringType)
				.field("time3", stringType);
		environment.connect(new FileSystem().path(file1))
				.withFormat(csv)
				.withSchema(new Schema()
						.field("order_no",stringType)
						.field("cust_order_no",stringType)
						.field("status",TypeInformation.of(Byte.class))
						.field("time1",stringType)
						.field("time2",stringType)
						.field("time3",stringType))
				.registerTableSource("orders");

		CsvTableSink sink = new CsvTableSink("E:\\project\\tmp\\output\\flink_sql",",",1, org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);
		environment.registerTableSink("order_group",new String[] { "status", "count_od" },
				new TypeInformation[] { TypeInformation.of(Byte.class), TypeInformation.of(Long.class)}
				,sink);

		final long start = System.nanoTime();
		//final Table table = environment.sqlQuery("select status,count(1) from orders group by status order by status");
		//table.insertInto("order_group");
		environment.sqlUpdate("insert into order_group select status,count(1) from orders group by status order by status");

		env.execute();

		System.out.println("excute time:" + (System.nanoTime() - start) / 1_000_000);
	}
}
