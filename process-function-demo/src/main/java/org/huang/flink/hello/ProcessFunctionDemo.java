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
import java.util.Collections;

public class ProcessFunctionDemo {
	public static void main(String[] args) {
		try {
			final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			int port = 9000;
			//TODO 启动监听服务, 测试先用nc处理吧
			//SocketServer socketServer = new SocketServer();
			//socketServer.startServer(port);

			//这里启动的是一个socket客户端去连接一个服务器的9000端口,而不是启动一个服务端监听9000端口
			DataStream<String> text = env.socketTextStream("localhost", port, "\n");
			//DataStream<String> text = env.fromCollection(Collections.singletonList("hello hello how r u how are your"));

			DataStream<Tuple2<String, Long>> windowCounts = text.process(new MyFlat())
					.keyBy(t -> t.f0).process(new Kpf());

			windowCounts.print();
			env.execute("Socket Window WordCount");
			//socketServer.stop();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static class MyFlat extends ProcessFunction<String,Tuple2<String, Long>> {
		@Override
		public void processElement(String value, Context ctx, Collector<Tuple2<String, Long>> out) {
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
							final String s = String.valueOf(Arrays.copyOfRange(chars, wordStart, i));
							out.collect(Tuple2.of(s,1L));
						}
						wordStart = i;
						strType = cType;
					}
				}
				if(wordStart < chars.length) {
					final String s = String.valueOf(Arrays.copyOfRange(chars, wordStart, chars.length));
					out.collect(Tuple2.of(s,1L));
				}
			}
		}
	}

	private static class Kpf extends KeyedProcessFunction<String,Tuple2<String, Long>,Tuple2<String, Long>> {
		private transient ValueState<Tuple2<String, Long>> state;
		@Override
		public void open(Configuration parameters) {
			state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState",Types.TUPLE(Types.STRING, Types.INT)));
		}

		/**
		 * 模拟一个5秒的窗口， 设定5000毫秒调用触发器， 触发器触发窗口输出并清空窗口状态
		 * @param value The input value.
		 * @param ctx A {@link Context} that allows querying the timestamp of the element and getting a
		 *     {@link TimerService} for registering timers and querying the time. The context is only
		 *     valid during the invocation of this method, do not store it.
		 * @param out The collector for returning result values.
		 * @throws Exception
		 */
		@Override
		public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out)
				throws Exception {
			System.out.println("--Kpf processElement: " + value);
			Tuple2<String, Long> current = state.value();
			if(current == null) {
				current = Tuple2.of(value.f0,1L);
				//这里指明触发器出发时间，满足时间则调用下方的onTimer方法
				ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 5000);
			} else {
				current.f1 = current.f1 + value.f1;
			}
			System.out.println("--Kpf state: " + current);
			state.update(current);
		}

		/**
		 * 当registerProcessingTimeTimer时间到达向下游输出状态数据并清空操作状态
		 * @param timestamp The timestamp of the firing timer.
		 * @param ctx An {@link OnTimerContext} that allows querying the timestamp, the {@link
		 *     TimeDomain}, and the key of the firing timer and getting a {@link TimerService} for
		 *     registering timers and querying the time. The context is only valid during the invocation
		 *     of this method, do not store it.
		 * @param out The collector for returning result values.
		 * @throws Exception
		 */
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
