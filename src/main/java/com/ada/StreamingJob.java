package com.ada;

import com.ada.Xie_function.STRTree.STRTree;
import com.ada.Xie_function.XieGlobalPF;
import com.ada.Xie_function.XieInputItemMF;
import com.ada.Xie_function.XieLocalPF;
import com.ada.common.Constants;
import com.ada.GQ_QBS_function.*;
import com.ada.geometry.Point;
import com.ada.geometry.Segment;
import com.ada.geometry.TrackPoint;
import com.ada.globalTree.GTree;
import com.ada.model.Xie.XieInputItem;
import com.ada.model.common.input.InputItem;
import com.ada.model.common.result.QueryResult;
import com.ada.random_function.IndexProcess;
import com.ada.random_function.SetKeyFlatMap;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;


public class StreamingJob {

	private static StreamExecutionEnvironment env;
	private static DataStream<InputItem> source;


	public static void main(String[] args) throws Exception {
		init();

//		DisIndexProcess();
//		randomParDisIndex();
		XieIndexProcess();

		env.execute("Distributed index");
	}

	private static void windowCount(){
		SimpleDateFormat format = new SimpleDateFormat("HH:mm");

		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		env.readTextFile("D:\\研究生资料\\track_data\\成都滴滴\\Sorted_2D\\XY_20161101")
				.map((MapFunction<String, TrackPoint>) TrackPoint::new)
				.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<TrackPoint>() {
					private long currentMaxTimestamp = 0;
					@Override
					public Watermark getCurrentWatermark() {
						return new Watermark(currentMaxTimestamp - 1);
					}
					@Override
					public long extractTimestamp(TrackPoint element, long previousElementTimestamp) {
						long timestamp = element.timestamp;
						currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
						return timestamp;
					}
				})
				.timeWindowAll(Time.minutes(30L), Time.minutes(10L))
				.process(new ProcessAllWindowFunction<TrackPoint, String, TimeWindow>() {
					@Override
					public void process(Context context, Iterable<TrackPoint> elements, Collector<String> out) throws Exception {
						int i = 0;
						for (TrackPoint ignored : elements) i++;
						out.collect(format.format(new Date(context.window().getStart())) + "\t" + i);
					}
				})
				.print()
		;
	}


	private static void init(){
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		source = env.readTextFile(Constants.dataPathParallel)
				.setParallelism(Constants.inputPartition)
				.flatMap(new ToInputItemFlatMap())
				.setParallelism(Constants.inputPartition)
				.assignTimestampsAndWatermarks(new InputItemTimeAndWater())
				.setParallelism(Constants.inputPartition)
		;
	}

	private static void randomParDisIndex() {
		source.flatMap(new SetKeyFlatMap())
				.keyBy("key")
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new IndexProcess())
				.setParallelism(Constants.dividePartition)
				.keyBy(value -> Constants.globalSubTaskKeyMap.get((int) value.getQueryID() % Constants.globalPartition))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new QueryResultPF(Constants.outPutPath + "random_result",
						"output"))
				.setParallelism(Constants.globalPartition)
				.print()
		;
	}

	private static void XieIndexProcess() throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(Constants.dataPathSingle + "XY_20161101"));
		String str = br.readLine();
		TrackPoint point;
		List<TrackPoint> list = new ArrayList<>(150000);
		do{
			assert str != null;
			point = new TrackPoint(str);
			list.add(point);
			str = br.readLine();
		} while (point.timestamp < Constants.winStartTime + Constants.windowSize*Constants.logicWindow && str != null);
		STRTree tree = new STRTree(list);
		source.map(new XieInputItemMF())
				.keyBy(value -> value.key)
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new XieGlobalPF(tree))
				.setParallelism(Constants.globalPartition)

				.keyBy(value -> value.key)
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new XieLocalPF())
				.setParallelism(Constants.dividePartition)

				.keyBy(value -> Constants.globalSubTaskKeyMap.get((int) value.getQueryID()%Constants.globalPartition))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new QueryResultPF(Constants.outPutPath + "Xie_result\\",
						"output"))
				.setParallelism(Constants.globalPartition)
				.print()
		;

	}

	private static void DisIndexProcess(){
        Jedis jedis = new Jedis("localhost");
		jedis.flushDB();
        jedis.flushAll();
        jedis.close();

		source.keyBy(value -> Constants.globalSubTaskKeyMap.get(value.getInputKey()%Constants.globalPartition))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new DensityPF())
				.setParallelism(Constants.globalPartition)

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
				.process(new QueryResultPF(Constants.outPutPath + "DSI_result\\",
						"output"))
				.setParallelism(Constants.globalPartition)

//				.forward()
//				.addSink(new WriteObjectSF<>("D:\\研究生资料\\论文\\my paper\\MyPaper\\分布式空间索引\\投递期刊\\Data\\debug\\DSI\\",
//						"output"))
				.print()
				;
	}
}
