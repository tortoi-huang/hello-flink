package org.huang.flink.hello;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class BatchWordCount {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<String> text;
		// get default test text data
		System.out.println("Executing WordCount example with default input data set.");
		System.out.println("Use --input to specify file1 input.");
		text = WordCountData.getDefaultTextLineDataSet(env);

		DataSet<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new Tokenizer())
						// group by the tuple field "0" and sum up tuple field "1"
						.groupBy(0)
						.sum(1);

		// emit result
		System.out.println("Printing result to stdout. Use --output to specify output path.");
		counts.print();

	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" ({@code Tuple2<String, Integer>}).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}
}
