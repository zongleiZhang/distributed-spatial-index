package com.ada;

import com.ada.common.Constants;
import com.ada.flinkFunction.*;
import com.ada.geometry.Segment;
import com.ada.model.Density;
import com.ada.model.DensityToGlobalElem;
import com.ada.model.GlobalToLocalElem;
import com.ada.proto.MyPoint;
import com.ada.geometry.TrackPoint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;


public class StreamingJob {

	public static void main(String[] args) throws Exception {
		DisIndexProcess();
	}

	private static void DisIndexProcess() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

		env.readTextFile("D:\\研究生资料\\论文\\track_data\\成都滴滴\\Sorted_2D\\XY_20161101")
				.map(TrackPoint::new)
				.assignTimestampsAndWatermarks(new TrackPointTimeAndWater())
				.partitionCustom((Partitioner<Integer>) (key, numPartitions) -> key%Constants.globalPartition, "TID")
				.flatMap(new TrackPointsToSegmentMap())
				.setParallelism(Constants.globalPartition)


				.keyBy(value -> Constants.globalSubTaskKeyMap.get(value.getTID()%Constants.globalPartition) )
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new DensityPF())
				.setParallelism(Constants.globalPartition)

				.keyBy(value -> Constants.globalSubTaskKeyMap.get(value.getDensityToGlobalKey()%Constants.globalPartition))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new GlobalTreePF())
				.setParallelism(Constants.globalPartition)
				.flatMap(new FlatMapFunction<GlobalToLocalElem, String>() {
					@Override
					public void flatMap(GlobalToLocalElem value, Collector<String> out) throws Exception {
						if (value.elementType == 10)
							out.collect("123");
					}
				})

//				.keyBy(value -> Constants.divideSubTaskKeyMap.get(value.key%Constants.dividePartition))
//				.timeWindow(Time.milliseconds(Constants.windowSize))
//				.process(new LocalTreePF())
//				.setParallelism(Constants.dividePartition)
//				.flatMap(new FlatMapFunction<String, String>() {
//					@Override
//					public void flatMap(String value, Collector<String> out) throws Exception {
//						if (value.length() == 1)
//							out.collect("123");
//					}
//				})


				.print()
				.setParallelism(1)
				;
		env.execute("Distributed index");
	}
}
