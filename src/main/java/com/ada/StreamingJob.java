package com.ada;

import com.ada.DTflinkFunction.*;
import com.ada.common.Constants;
import com.ada.model.TwoThreeData;
import com.ada.geometry.TrackPoint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class StreamingJob {

	public static void main(String[] args) throws Exception {
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
				.map(TrackPoint::new)
				.assignTimestampsAndWatermarks(new TrackPointTAndW())


				.keyBy(new RebalanceKeySelector<>(Constants.densityPartition))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new DensityPF())
				.setParallelism(Constants.densityPartition)

				.flatMap(new WaterAndDensityFMP())
				.setParallelism(Constants.inputPartition)
				.partitionCustom((Partitioner<Integer>) (key, numPartitions) -> key, "key")
				.flatMap(new HausdorffKeyTIDFunction())
				.setParallelism(Constants.keyTIDPartition)
				.flatMap(new FlatMapFunction<TwoThreeData, String>() {
					@Override
					public void flatMap(TwoThreeData value, Collector<String> out) throws Exception {
						if (false)
							out.collect("132");
					}
				})
//				.partitionCustom((Partitioner<Integer>) (key, numPartitions) -> Constants.divideSubTaskKayMap.get(key), "key")
//				.flatMap(new HausdorffDivideMF())
//				.setParallelism(Constants.dividePartition)

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
