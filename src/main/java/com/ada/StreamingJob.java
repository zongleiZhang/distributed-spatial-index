package com.ada;

import com.ada.flinkFunction.DPIflinkFunction.TestAWF;
import com.ada.flinkFunction.DPIflinkFunction.TrackPointsToSegmentMap;
import com.ada.flinkFunction.DTflinkFunction.HausdorffKeyTIDFunction;
import com.ada.flinkFunction.TrackPointTimeAndWater;
import com.ada.flinkFunction.DTflinkFunction.WaterAndDensityFMP;
import com.ada.common.Constants;
import com.ada.dispatchElem.TwoThreeData;
import com.ada.proto.MyPoint;
import com.ada.trackSimilar.TrackPoint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Properties;


public class StreamingJob {
	private static StreamExecutionEnvironment env = null;
	private static DataStream<TrackPoint> source = null;

	public static void main(String[] args) throws Exception {
		DisIndexProcess();
		DisTrackProcess();
	}


	private static void DisTrackProcess() throws Exception {
		init( "trackPoint2000", "192.168.100.1:9092");

//        init( "Point5000", "192.168.131.199:9092,192.168.131.199:9093,192.168.131.199:9094");

		source
				.flatMap(new WaterAndDensityFMP())
				.setParallelism(Constants.topicPartition)
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


	private static void DisIndexProcess() throws Exception {
		init( "trackPoint1", "10.10.0.1:9092,10.10.0.2:9092");

//        init( "trackPoint3DT", "192.168.131.199:9093,192.168.131.199:9094,192.168.131.199:9095");

		source.assignTimestampsAndWatermarks(new TrackPointTimeAndWater())
//				.flatMap(new TrackPointsToSegmentMap())
//				.setParallelism(Constants.topicPartition)
				.timeWindowAll(Time.seconds(Constants.windowSize))
				.process(new TestAWF())

//				.keyBy((KeySelector<Segment, Integer>) value -> Constants.globalSubTaskKayMap.get(value.getTID()%Constants.globalPartition) )
//				.timeWindow(Time.seconds(Constants.windowSize))
//				.process(new GlobalTreePF())
//				.setParallelism(Constants.globalPartition)
//				.keyBy(0/*(KeySelector<Tuple2<Integer, Segment>, Integer>) value -> value.f0*/)
//				.timeWindow(Time.seconds(Constants.windowSize))
//				.process(new LocalTreePF())
//				.setParallelism(Constants.dividePartition)

//				.writeAsText("/home/chenliang/data/zzlDI/mutiNodeRES" + Constants.logicWindow /*+".txt"*/, FileSystem.WriteMode.OVERWRITE)
				.writeAsText("F:\\data\\oneNodeRES" + Constants.logicWindow +".txt", FileSystem.WriteMode.OVERWRITE)
//				.print()
				.setParallelism(1/*Constants.dividePartition*/);
		env.execute("Distributed index");
	}

	private static void testGlocalTree() throws Exception {

		init( "trackPoint111", "10.10.0.1:9092,10.10.0.2:9092,10.10.0.3:9092");

		source.assignTimestampsAndWatermarks(new TrackPointTimeAndWater())
				.setParallelism(1)
				.flatMap(new TrackPointsToSegmentMap())
				.setParallelism(1)
				.timeWindowAll(Time.milliseconds(Constants.windowSize))
				.process(new TestAWF())
				.print()
				.setParallelism(1);

		env.execute("Distributed index");
	}

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
