package com.ada.GQ_QBS_Function;

import com.ada.QBSTree.RCtree;
import com.ada.common.Constants;
import com.ada.geometry.Segment;
import com.ada.model.inputItem.InputItem;
import com.ada.model.inputItem.QueryItem;
import com.ada.proto.MyResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

public class SingleNodeIndexPF  extends ProcessAllWindowFunction<InputItem, String, TimeWindow> {
    private RCtree<Segment> index;
    private Map<Long, List<Segment>> segmentsMap;

    private String path;
    private String prefix;
    private FileOutputStream fos;
    private MyResult.QueryResult.TrackPoint.Builder tpBuilder;
    private MyResult.QueryResult.Segment.Builder sBuilder;
    private MyResult.QueryResult.Builder qrBuilder;

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
            qrBuilder.clear().setQueryID(queryItem.queryID).setTimeStamp(queryItem.timeStamp);
            for (Segment seg : index.<Segment>rectQuery(queryItem.rect, false)) {
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
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        index =  new RCtree<>(4,1,11, Constants.globalRegion,0);
        segmentsMap = new HashMap<>();

        File f = new File(path, prefix + "_" +getRuntimeContext().getIndexOfThisSubtask());
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
