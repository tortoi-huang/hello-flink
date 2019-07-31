package org.huang.flink.hello;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class MysqlReport {

	public static void main(String[] args) throws Exception{
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final DataStreamSource<Row> input = env.createInput(getInput());
		input.print();
	}

	private static JDBCInputFormat getInput() {
		TypeInformation[] fieldTypes =new TypeInformation[]{
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.BYTE_TYPE_INFO,
				BasicTypeInfo.DATE_TYPE_INFO
		};
		return JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername("com.mysql.jdbc.Driver")
				.setDBUrl("jdbc:mysql://10.199.201.38:3306/financial?useUnicode=true&amp;characterEncoding=utf-8&amp;zeroDateTimeBehavior=convertToNull")
				.setUsername("root").setPassword("pjbest123")
				.setQuery("select id,cust_order_no,product_type,order_time from inf_delivery_order limit 100")
				.setRowTypeInfo(new RowTypeInfo(fieldTypes))
				.finish();
	}
}
