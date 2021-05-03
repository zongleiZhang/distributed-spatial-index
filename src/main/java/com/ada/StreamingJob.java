package com.ada;

import com.ada.GQ_QBS_function.*;
import com.ada.Xie_function.STRTree.STRTree;
import com.ada.Xie_function.XieGlobalPF;
import com.ada.Xie_function.XieInputItemMF;
import com.ada.Xie_function.XieLocalPF;
import com.ada.common.Constants;
import com.ada.geometry.Segment;
import com.ada.geometry.TrackPoint;
import com.ada.model.common.input.InputItem;
import com.ada.random_function.RandomIndexProcess;
import com.ada.random_function.RandomInputItemFMP;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class StreamingJob {

	private static StreamExecutionEnvironment env;
	private static DataStream<InputItem> source;


	public static void main(String[] args) throws Exception {
//		windowCount();

		init();
        if ("DIP".equals(Constants.frame)){
            DisIndexProcess();
        }else if ("Xie".equals(Constants.frame)){
            XieIndexProcess();
        }else if ("random".equals(Constants.frame)){
            randomParDisIndex();
        }else {
            throw new IllegalArgumentException("frame error.");
        }

		env.execute(Constants.frame + " Distributed index");
	}

	private static void windowCount(){
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm");

		init();
		source.timeWindowAll(Time.minutes(10L))
				.process(new ProcessAllWindowFunction<InputItem, String, TimeWindow>() {
					@Override
					public void process(Context context, Iterable<InputItem> elements, Collector<String> out) {
						int indexItemNum = 0;
						int queryItemNum = 0;
						for (InputItem element: elements){
							if (element instanceof Segment){
								indexItemNum++;
							}else {
								queryItemNum++;
							}
						}
						out.collect(format.format(new Date(context.window().getStart())) + "\t" + indexItemNum + "\t" + queryItemNum);
					}
				})
				.print()
		;

//		env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//		env.setParallelism(1);
//		env.readTextFile(Constants.dataSingleFileName)
//				.map((MapFunction<String, TrackPoint>) TrackPoint::new)
//				.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<TrackPoint>() {
//					private long currentMaxTimestamp = 0;
//					@Override
//					public Watermark getCurrentWatermark() {
//						return new Watermark(currentMaxTimestamp - 1);
//					}
//					@Override
//					public long extractTimestamp(TrackPoint element, long previousElementTimestamp) {
//						long timestamp = element.timestamp;
//						currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
//						return timestamp;
//					}
//				})
//				.timeWindowAll(Time.minutes(10L))
//				.process(new ProcessAllWindowFunction<TrackPoint, String, TimeWindow>() {
//					@Override
//					public void process(Context context, Iterable<TrackPoint> elements, Collector<String> out) throws Exception {
//						int i = 0;
//						for (TrackPoint ignored : elements) i++;
//						out.collect(format.format(new Date(context.window().getStart())) + "\t" + i);
//					}
//				})
//				.print()
//		;
	}


	private static void init(){
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//		Properties properties = new Properties();
//		properties.setProperty("bootstrap.servers", "192.168.131.199:9092");
//		FlinkKafkaConsumer011<String> myConsumer =
//				new FlinkKafkaConsumer011<>(Constants.topic, new SimpleStringSchema(), properties);
//		myConsumer.setStartFromEarliest();
////		myConsumer.setStartFromLatest();  //读最新的
//		source = env.addSource(myConsumer)

		source = env.readTextFile(Constants.dataSingleFileName)
				.setParallelism(Constants.inputPartition)
				.flatMap(new ToInputItemFlatMap())
				.setParallelism(Constants.inputPartition)
				.assignTimestampsAndWatermarks(new InputItemTimeAndWater())
				.setParallelism(Constants.inputPartition)
		;
	}

	private static void randomParDisIndex() {
		source.flatMap(new RandomInputItemFMP())
				.setParallelism(Constants.inputPartition)
				.keyBy("key")
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new RandomIndexProcess())
				.setParallelism(Constants.dividePartition)
				.keyBy(value -> Constants.globalSubTaskKeyMap.get((int) value.getQueryID() % Constants.globalPartition))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new QueryResultPF(Constants.outPutPath + "random_result",
						"output"))
				.setParallelism(Constants.globalPartition)
				.print()
				.setParallelism(1)
		;
	}

	private static void XieIndexProcess() throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(Constants.dataSingleFileName));
		String str = br.readLine();
		TrackPoint point;
		List<TrackPoint> list = new ArrayList<>(150000);
		do{
			assert str != null;
			point = new TrackPoint(str);
			list.add(point);
			str = br.readLine();
		} while (point.timestamp < Constants.winStartTime + Constants.windowSize*Constants.logicWindow && str != null);
		br.close();
		STRTree tree = new STRTree(list);
		source.map(new XieInputItemMF())
				.setParallelism(Constants.inputPartition)
				.keyBy("key")
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new XieGlobalPF(tree))
				.setParallelism(Constants.globalPartition)

				.keyBy("key")
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new XieLocalPF())
				.setParallelism(Constants.dividePartition)

				.keyBy(value -> Constants.globalSubTaskKeyMap.get((int) value.getQueryID()%Constants.globalPartition))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new QueryResultPF(Constants.outPutPath + "Xie_result",
						"output"))
				.setParallelism(Constants.globalPartition)
				.print()
				.setParallelism(1)
		;

	}

	private static void DisIndexProcess(){
        Jedis jedis = new Jedis("localhost");
		jedis.flushDB();
        jedis.flushAll();
        jedis.close();

//		source.keyBy(value -> Constants.globalSubTaskKeyMap.get(value.getInputKey()%Constants.globalPartition))
//				.timeWindow(Time.milliseconds(Constants.windowSize))
//				.process(new DensityPF())
//				.setParallelism(Constants.globalPartition)
		source.timeWindowAll(Time.milliseconds(Constants.windowSize))
				.process(new DensityPF())

				.keyBy(value -> Constants.globalSubTaskKeyMap.get(value.getD2GKey()%Constants.globalPartition))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new GlobalTreePF())
				.setParallelism(Constants.globalPartition)

				.keyBy(value -> Constants.divideSubTaskKeyMap.get(value.key%Constants.dividePartition))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new LocalTreePF())
				.setParallelism(Constants.dividePartition)

                .keyBy(value -> Constants.globalSubTaskKeyMap.get((int) value.getQueryID()%Constants.globalPartition))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new QueryResultPF(Constants.outPutPath + "DSI_result",
						"output"))
				.setParallelism(Constants.globalPartition)

				.print()
				.setParallelism(1)
				;
	}
}
