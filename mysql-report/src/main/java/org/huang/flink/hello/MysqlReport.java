package org.huang.flink.hello;

public class MysqlReport {

	public static void main(String[] args) {
		/*final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final DataStreamSource<Row> input = env.createInput(getInput());
		input.print();*/
	}

	/*private static JDBCInputFormat getInput() {
		TypeInformation[] fieldTypes = new TypeInformation[] { BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.BYTE_TYPE_INFO, BasicTypeInfo.DATE_TYPE_INFO };
		return JDBCInputFormat.buildJDBCInputFormat().setDrivername("com.mysql.jdbc.Driver").setDBUrl(
				"jdbc:mysql://10.199.201.38:3306/financial?useUnicode=true&amp;characterEncoding=utf-8&amp;zeroDateTimeBehavior=convertToNull")
				.setUsername("root").setPassword("pjbest123")
				.setQuery("select id,cust_order_no,product_type,order_time from inf_delivery_order limit 100")
				.setRowTypeInfo(new RowTypeInfo(fieldTypes)).finish();
	}*/
}
