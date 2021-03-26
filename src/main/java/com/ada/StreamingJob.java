package com.ada;

import com.ada.flinkFunction.*;
import com.ada.common.Constants;
import com.ada.model.globalToLocal.G2LElem;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;


public class StreamingJob {

	public static void main(String[] args) throws Exception {
		Jedis jedis = new Jedis("localhost");
		jedis.flushDB();
		jedis.flushAll();
		jedis.close();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

//		DataStream<TrackPoint> source;
//		Properties properties = new Properties();
//		properties.setProperty("bootstrap.servers", "192.168.100.1:9092");
//		FlinkKafkaConsumer011<TrackPoint> myConsumer =
//				new FlinkKafkaConsumer011<>("trackPoint2000",new AbstractDeserializationSchema<TrackPoint>() {
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


		env.readTextFile("D:\\研究生资料\\track_data\\成都滴滴\\Sorted_2D\\XY_20161101")
				.map(new MapToTrackPoint())
				.assignTimestampsAndWatermarks(new TrackPointTAndW())

				.keyBy(value -> Constants.densitySubTaskKeyMap.get(value.key))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new DensityPF())
				.setParallelism(Constants.densityPartition)
//				.flatMap(new FlatMapFunction<D2GElem, String >() {
//					@Override
//					public void flatMap(D2GElem value, Collector<String> out) throws Exception {
//						if (value.getD2GKey() == -1)
//							out.collect("123");
//					}
//				})

				.keyBy(value -> Constants.globalSubTaskKeyMap.get(value.getD2GKey()))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new HausdorffGlobalPF())
				.setParallelism(Constants.globalPartition)
//				.flatMap(new FlatMapFunction<G2LElem, String>() {
//					@Override
//					public void flatMap(G2LElem value, Collector<String> out) {
//						if (value.flag == -1){
//							out.collect("123");
//						}
//					}
//				})

				.keyBy(value -> Constants.divideSubTaskKeyMap.get(value.key))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new HausdorffLocalPF())
				.setParallelism(Constants.dividePartition)

//				.flatMap(new HausdorffOneNodeMF())
//				.setParallelism(1)

				.print()
//				.writeAsText("/home/chenliang/data/zzlDI/" +
//						Constants.logicWindow + "_" +
//						Constants.t + "_" +
//						".txt", FileSystem.WriteMode.OVERWRITE)
//				.setParallelism(1/*Constants.dividePartition*/)
		;
		env.execute("logicWindow: " + Constants.logicWindow +
				" t: " + Constants.t +
				" topK: " + Constants.topK);
	}
}
