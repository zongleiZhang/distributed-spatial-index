package com.ada.GQ_QBS_function;

import com.ada.QBSTree.RCtree;
import com.ada.common.Constants;
import com.ada.geometry.Segment;
import com.ada.model.common.input.InputItem;
import com.ada.model.common.input.QueryItem;
import com.ada.model.common.result.QueryResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

public class SingleNodeIndexPF  extends ProcessAllWindowFunction<InputItem, String, TimeWindow> {
    private RCtree<Segment> index;
    private Map<Long, List<Segment>> segmentsMap;

    private String path;
    private String prefix;
    private ObjectOutputStream oos;

    private int count = 0;

    public SingleNodeIndexPF(String path, String prefix){
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
    public void process(Context context, Iterable<InputItem> elements, Collector<String> out) throws IOException {
        long logicWinStart = context.window().getEnd() - Constants.logicWindow*Constants.windowSize;
        if (logicWinStart < 0)
            out.collect("123");
        System.out.println(count);
        count++;

        Iterator<Map.Entry<Long, List<Segment>>> ite = segmentsMap.entrySet().iterator();
        while (ite.hasNext()){
            Map.Entry<Long, List<Segment>> entry = ite.next();
            if (entry.getKey() < logicWinStart){
                ite.remove();
                for (Segment segment : entry.getValue())
                    index.delete(segment);
            }
        }

        List<QueryItem> queryItems = new ArrayList<>();
        List<Segment> indexItems = new ArrayList<>();
        for (InputItem element : elements) {
            if (element instanceof Segment){
                Segment segment = (Segment) element;
                index.insert(segment);
                indexItems.add(segment);
            }else {
                queryItems.add((QueryItem) element);
            }
        }
        segmentsMap.put(context.window().getStart(), indexItems);
        for (QueryItem queryItem : queryItems) {
            QueryResult result = new QueryResult(queryItem.queryID, queryItem.inputTime, 0L, index.rectQuery(queryItem.rect, false));
            oos.writeObject(result);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        index =  new RCtree<>(4,1,11, Constants.globalRegion,0);
        segmentsMap = new HashMap<>();

        File f = new File(path, prefix + "_" +getRuntimeContext().getIndexOfThisSubtask());
        if (!f.exists()) f.createNewFile();
        oos = new ObjectOutputStream(new FileOutputStream(f));
    }

    @Override
    public void close() throws Exception {
        super.close();
        oos.close();
    }
}
