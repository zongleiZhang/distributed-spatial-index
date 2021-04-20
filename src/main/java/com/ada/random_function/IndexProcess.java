package com.ada.random_function;

import com.ada.QBSTree.RCtree;
import com.ada.common.Constants;
import com.ada.geometry.Segment;
import com.ada.model.inputItem.QueryItem;
import com.ada.model.result.QueryResult;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class IndexProcess extends ProcessWindowFunction<InputItemKey, QueryResult, Tuple, TimeWindow> {
    private Deque<List<Segment>> queue;
    private RCtree<Segment> index;

    @Override
    public void process(Tuple key,
                        Context context,
                        Iterable<InputItemKey> elements,
                        Collector<QueryResult> out) {
        List<QueryItem> queryItems = new ArrayList<>();
        List<Segment> indexItems = new ArrayList<>();
        for (InputItemKey element : elements) {
            if (element.item instanceof Segment){
                Segment segment = (Segment) element.item;
                indexItems.add(segment);
                index.insert(segment);
            }else {
                queryItems.add((QueryItem) element.item);
            }
        }
        queue.offer(indexItems);
        if (queue.getFirst().get(0).getTimeStamp() <
                context.window().getEnd() - Constants.logicWindow*Constants.windowSize){
            for (Segment segment : queue.poll()) index.delete(segment);
        }
        for (QueryItem queryItem : queryItems) {
            List<Segment> result = index.rectQuery(queryItem.rect, false);
            out.collect(new QueryResult(queryItem.queryID, queryItem.timeStamp, result));
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        queue = new ArrayDeque<>();
        index = new RCtree<>(4,1,11, Constants.globalRegion.clone(),0);
    }
}



































