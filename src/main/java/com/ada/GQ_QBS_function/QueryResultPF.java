package com.ada.GQ_QBS_function;

import com.ada.geometry.Segment;
import com.ada.model.common.result.QueryResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.*;

public class QueryResultPF extends ProcessWindowFunction<QueryResult, QueryResult, Integer, TimeWindow> {
    private String path;
    private String prefix;
    private ObjectOutputStream oos;
    private int subTask;
    private SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss");

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
            QueryResult result = new QueryResult(entry.getKey(), entry.getValue().f0, new ArrayList<>(entry.getValue().f1));
            oos.writeObject(result);
        }
        oos.flush();
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
        oos = new ObjectOutputStream(new FileOutputStream(f));
    }

    @Override
    public void close() throws Exception {
        super.close();
        oos.close();
    }
}



























