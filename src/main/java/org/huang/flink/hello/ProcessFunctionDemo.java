package org.huang.flink.hello;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class ProcessFunctionDemo {
	public static void main(String[] args) {
		try {
			final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");

			DataStream<Tuple2<String, Long>> windowCounts = text.process(new MyFlat())
					.keyBy(0).process(new Kpf());

			windowCounts.print();
			env.execute("Socket Window WordCount");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static class MyFlat extends ProcessFunction<String,Tuple2<String, Long>> {
		@Override
		public void processElement(String value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
			if(value != null && value.length() > 0) {
				final char[] chars = value.toCharArray();
				int wordStart = 0;
				int strType = 0;
				for (int i = 0;i < chars.length;i++) {
					int cType;
					if(chars[i] <= ' ') {
						cType = 1;
					} else if(chars[i] >= '0' && chars[i] <= '9' || chars[i] >= 'A' && chars[i] <= 'Z'
							|| chars[i] >= 'a' && chars[i] <= 'z' || chars[i] > 127) {
						cType = 2;
					} else {
						cType = 3;
					}
					if(strType == 0) {
						strType = cType;
					} else if(strType != cType) {
						if(strType != 1) {
							final String s = new String(Arrays.copyOfRange(chars, wordStart, i));
							out.collect(Tuple2.of(s,1L));
						}
						wordStart = i;
						strType = cType;
					}
				}
				if(wordStart < chars.length) {
					final String s = new String(Arrays.copyOfRange(chars, wordStart, chars.length));
					out.collect(Tuple2.of(s,1L));
				}
			}
		}
	}

	private static class Kpf extends KeyedProcessFunction<Tuple,Tuple2<String, Long>,Tuple2<String, Long>> {
		private ValueState<Tuple2<String, Long>> state;
		@Override
		public void open(Configuration parameters) throws Exception {
			state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState",Types.TUPLE(Types.STRING, Types.INT)));
		}

		@Override
		public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out)
				throws Exception {
			System.out.println("--processElement: " + value);
			Tuple2<String, Long> current = state.value();
			if(current == null) {
				current = Tuple2.of(value.f0,1L);
				ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 5000);
			} else {
				current.f1 = current.f1 + value.f1;
			}
			state.update(current);
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
			Tuple2<String, Long> current = state.value();
			if(current != null) {
				out.collect(current);
				state.clear();
			}
		}
	}
}
