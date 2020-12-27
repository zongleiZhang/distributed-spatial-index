package com.ada;

import com.ada.QBSTree.RCtree;
import com.ada.common.Constants;
import com.ada.flinkFunction.*;
import com.ada.geometry.Segment;
import com.ada.model.densityToGlobal.DensityToGlobalElem;
import com.ada.model.globalToLocal.GlobalToLocalElem;
import com.ada.model.inputItem.InputItem;
import com.ada.model.result.QueryResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;


public class StreamingJob {

	private static StreamExecutionEnvironment env;
	private static DataStream<String> source;


	public static void main(String[] args) throws Exception {
		init();
//		singleNodeIndex();
		DisIndexProcess();
	}


	private static void init(){
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		source = env.readTextFile("D:\\研究生资料\\track_data\\成都滴滴\\XY_20161101_mn")
				.setParallelism(Constants.inputPartition);

//		Properties properties = new Properties();
//		properties.setProperty("bootstrap.servers", bootstrap);
//		FlinkKafkaConsumer011<TrackPoint> myConsumer =
//				new FlinkKafkaConsumer011<>(topic,new AbstractDeserializationSchema<TrackPoint>() {
//					@Override
//					public TrackPoint deserialize(byte[] message) throws IOException {
//						MyPoint.Point myPoint = MyPoint.Point.parseFrom(message);
//						return new TrackPoint(new double[]{myPoint.getLon(), myPoint.getLat()}, myPoint.getTimeStamp(), myPoint.getTID()+1);
//					}
//				}, properties);
//		myConsumer.setStartFromEarliest();
////		myConsumer.setStartFromLatest();  //读最新的
//		source = env.addSource(myConsumer)
//				.setParallelism(Constants.topicPartition);
	}

	private static void singleNodeIndex() throws Exception {
		source.flatMap(new ToInputItemFlatMap())
				.assignTimestampsAndWatermarks(new InputItemTimeAndWater())
				.timeWindowAll(Time.milliseconds(Constants.windowSize))
				.process(new SingleNodeIndexPF("D:\\研究生资料\\论文\\my paper\\MyPaper\\分布式空间索引\\投递期刊\\Data\\debug\\SSI_QBS",
						"output"))
				.print()
		;
		env.execute("single node index");

	}

	private static void DisIndexProcess() throws Exception {
        Jedis jedis = new Jedis("localhost");
		jedis.flushDB();
        jedis.flushAll();
        jedis.close();

		source.flatMap(new ToInputItemFlatMap())
				.assignTimestampsAndWatermarks(new InputItemTimeAndWater())

				.keyBy(value -> Constants.globalSubTaskKeyMap.get(value.getInputKey()%Constants.globalPartition))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new DensityPF())
				.setParallelism(Constants.globalPartition)

				.keyBy(value -> Constants.globalSubTaskKeyMap.get(value.getD2GKey()%Constants.globalPartition))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new GlobalTreePF())
				.setParallelism(Constants.globalPartition)
//				.flatMap(new FlatMapFunction<GlobalToLocalElem, String>() {
//					@Override
//					public void flatMap(GlobalToLocalElem value, Collector<String> out) throws Exception {
//						if (value.elementType == 10)
//							out.collect("123");
//					}
//				})

				.keyBy(value -> Constants.divideSubTaskKeyMap.get(value.key%Constants.dividePartition))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new LocalTreePF())
				.setParallelism(Constants.dividePartition)
//				.flatMap(new FlatMapFunction<QueryResult, String>() {
//					@Override
//					public void flatMap(QueryResult value, Collector<String> out) throws Exception {
//						if (value.timeStamp == -1L)
//							out.collect("123");
//					}
//				})

                .keyBy(value -> Constants.globalSubTaskKeyMap.get((int) value.getQueryID()%Constants.globalPartition))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new QueryResultPF("D:\\研究生资料\\论文\\my paper\\MyPaper\\分布式空间索引\\投递期刊\\Data\\debug\\DSI\\",
						"output"))
				.setParallelism(Constants.globalPartition)

//				.forward()
//				.addSink(new WriteObjectSF<>("D:\\研究生资料\\论文\\my paper\\MyPaper\\分布式空间索引\\投递期刊\\Data\\debug\\DSI\\",
//						"output"))
				.print()
				;
		env.execute("Distributed index");
	}
}
