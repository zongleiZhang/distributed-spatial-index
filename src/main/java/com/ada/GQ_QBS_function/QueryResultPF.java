package com.ada.GQ_QBS_function;

import com.ada.geometry.Segment;
import com.ada.model.common.result.QueryResult;
import com.ada.proto.MyResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class QueryResultPF extends ProcessWindowFunction<QueryResult, QueryResult, Integer, TimeWindow> {
    private String path;
    private String prefix;
    private FileOutputStream fos;
    private MyResult.QueryResult.TrackPoint.Builder tpBuilder;
    private MyResult.QueryResult.Segment.Builder sBuilder;
    private MyResult.QueryResult.Builder qrBuilder;
    private int subTask;
    SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss");

    public QueryResultPF(String path, String prefix){
        File f = new File(path);
        if (!f.exists()){
            f.mkdir();
        } else {
            for (File file : f.listFiles()) {
                file.delete();
            }
        }
        this.path = path;
        this.prefix = prefix;
    }

    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<QueryResult> elements,
                        Collector<QueryResult> out) throws IOException {
        Map<Long, Tuple2<Long, Set<Segment>>> map = new HashMap<>();
        for (QueryResult element : elements) {
            Tuple2<Long, Set<Segment>> tuple2 = map.computeIfAbsent(element.queryID, s -> new Tuple2<>(element.timeStamp, new HashSet<>()));
            tuple2.f1.addAll(element.list);
        }
        for (Map.Entry<Long, Tuple2<Long, Set<Segment>>> entry : map.entrySet()) {
            qrBuilder.clear().setQueryID(entry.getKey()).setTimeStamp(entry.getValue().f0);
            for (Segment seg : entry.getValue().f1) {
                tpBuilder.clear().setTID(seg.p1.TID).setTimeStamp(seg.p1.timestamp).setX(seg.p1.data[0]).setY(seg.p1.data[1]);
                MyResult.QueryResult.TrackPoint tp1 =  tpBuilder.build();
                tpBuilder.clear().setTID(seg.p2.TID).setTimeStamp(seg.p2.timestamp).setX(seg.p2.data[0]).setY(seg.p2.data[1]);
                MyResult.QueryResult.TrackPoint tp2 =  tpBuilder.build();
                sBuilder.clear().setP1(tp1).setP2(tp2);
                MyResult.QueryResult.Segment proSegment = sBuilder.build();
                qrBuilder.addList(proSegment);
            }
            MyResult.QueryResult result = qrBuilder.build();
            result.writeDelimitedTo(fos);
        }
        fos.flush();
        if (subTask == 0 && (context.window().getEnd() - 1477929780000L)%600000 == 0)
            System.out.println(ft.format(new Date(context.window().getEnd())));
        if (map.size() == 0) out.collect(null);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        subTask = getRuntimeContext().getIndexOfThisSubtask();
        File f = new File(path, prefix + "_" + subTask);
        if (!f.exists()) f.createNewFile();
        fos = new FileOutputStream(f);
        tpBuilder = MyResult.QueryResult.TrackPoint.newBuilder();
        sBuilder = MyResult.QueryResult.Segment.newBuilder();
        qrBuilder = MyResult.QueryResult.newBuilder();
    }

    @Override
    public void close() throws Exception {
        super.close();
        fos.close();
    }
}



























