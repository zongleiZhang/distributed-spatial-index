package com.ada;

import com.ada.flinkFunction.TrackPointsToSegmentMap;
import com.ada.flinkFunction.TrackPointTimeAndWater;
import com.ada.common.Constants;
import com.ada.proto.MyPoint;
import com.ada.geometry.Segment;
import com.ada.geometry.TrackPoint;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


public class StreamingJob {
	private static StreamExecutionEnvironment env = null;
	private static DataStream<TrackPoint> source = null;

	public static void main(String[] args) throws Exception {
		Properties pro = new Properties();
		FileInputStream in = new FileInputStream("conf.properties");
		pro.load(in);
		in.close();
//		DisIndexProcess();
	}

//	private static void DisIndexProcess() throws Exception {
//		init( "trackPoint1", "10.10.0.1:9092,10.10.0.2:9092");
//
////        init( "trackPoint3DT", "192.168.131.199:9093,192.168.131.199:9094,192.168.131.199:9095");
//
//		source.assignTimestampsAndWatermarks(new TrackPointTimeAndWater())
//				.partitionCustom((Partitioner<Integer>) (key, numPartitions) -> key%numPartitions, "TID")
//				.flatMap(new TrackPointsToSegmentMap())
//				.keyBy((KeySelector<Segment, Integer>) value -> Constants.globalSubTaskKeyMap.get(value.getTID()%Constants.globalPartition) )
//				.timeWindow(Time.seconds(Constants.windowSize))
////				.process()
//
////				.process(new GlobalTreePF())
////				.setParallelism(Constants.globalPartition)
////				.keyBy(0/*(KeySelector<Tuple2<Integer, Segment>, Integer>) value -> value.f0*/)
////				.timeWindow(Time.seconds(Constants.windowSize))
////				.process(new LocalTreePF())
////				.setParallelism(Constants.dividePartition)
////				.writeAsText("/home/chenliang/data/zzlDI/mutiNodeRES" + Constants.logicWindow /*+".txt"*/, FileSystem.WriteMode.OVERWRITE)
////				.writeAsText("F:\\data\\oneNodeRES" + Constants.logicWindow +".txt", FileSystem.WriteMode.OVERWRITE)
////				.print()
////				.setParallelism(1/*Constants.dividePartition*/)
//				;
//		env.execute("Distributed index");
//	}


	private static void init( String topic, String bootstrap) throws Exception {
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", bootstrap);
		FlinkKafkaConsumer011<TrackPoint> myConsumer =
				new FlinkKafkaConsumer011<>(topic,new AbstractDeserializationSchema<TrackPoint>() {
					@Override
					public TrackPoint deserialize(byte[] message) throws IOException {
						MyPoint.Point myPoint = MyPoint.Point.parseFrom(message);
						return new TrackPoint(new double[]{myPoint.getLon(), myPoint.getLat()}, myPoint.getTimeStamp(), myPoint.getTID()+1);
					}
				}, properties);
		myConsumer.setStartFromEarliest();
//		myConsumer.setStartFromLatest();  //读最新的
		source = env.addSource(myConsumer)
				.setParallelism(Constants.topicPartition);
	}
}
