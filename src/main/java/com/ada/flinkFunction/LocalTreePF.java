package com.ada.flinkFunction;

import com.ada.QBSTree.RCDataNode;
import com.ada.QBSTree.RCtree;
import com.ada.common.Arrays;
import com.ada.common.Constants;
import com.ada.geometry.GridPoint;
import com.ada.geometry.GridRectangle;
import com.ada.geometry.Rectangle;
import com.ada.geometry.Segment;
import com.ada.model.GlobalToLocalElem;
import com.ada.model.LocalRegionAdjustInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class LocalTreePF extends ProcessWindowFunction<GlobalToLocalElem, String, Integer, TimeWindow> {
    private boolean isFirst;
    private int subTask;
    private RCtree<Segment> localIndex;
    private Map<Long,List<Segment>> segmentsMap;
    private Jedis jedis;


    @Override
    public void process(Integer key, Context context, Iterable<GlobalToLocalElem> elements, Collector<String> out) throws Exception {
        long startWindow = context.window().getStart();

        //将输入数据分类
        List<Rectangle> queryItems = new ArrayList<>();
        List<Segment> indexItems = new ArrayList<>();
        LocalRegionAdjustInfo adjustInfo = null;
        for (GlobalToLocalElem elem : elements) {
            if (elem.elementType == 1) {
                indexItems.add((Segment) elem.value);
            } else if (elem.elementType == 2) {
                queryItems.add((Rectangle) elem.value);
            } else {
                adjustInfo = (LocalRegionAdjustInfo) elem.value;
            }
        }

        if (isFirst) {
            assert adjustInfo != null;
            openThisSubTask(adjustInfo);
            isFirst = false;
        }

        for (Segment item : indexItems) {
            localIndex.insert(item);
        }
        segmentsMap.put(startWindow, indexItems);
        for (Rectangle rectangle : queryItems)
            queryResToString(out, rectangle);

//        segmentssCheck();
        //从索引中移除过时数据
        long logicStartTime = startWindow - Constants.logicWindow*Constants.windowSize;
        Iterator<Map.Entry<Long, List<Segment>>> ite = segmentsMap.entrySet().iterator();
        while (ite.hasNext()){
            Map.Entry<Long, List<Segment>> entry = ite.next();
            if (entry.getKey() < logicStartTime){
                ite.remove();
                for (Segment segment : entry.getValue())
                    localIndex.delete(segment);
                break;
            }

        }


        //需要数据迁移
        if (adjustInfo != null){
            //将需要迁出的数据添加到Redis中
            if (adjustInfo.migrateOutST != null){
                for (Tuple2<Integer, Rectangle> tuple2 : adjustInfo.migrateOutST) {
                    List<Segment> segments = localIndex.rectQuery(tuple2.f1, false);
                    for (Segment segment : segments) {
                        Long time = startWindow - ((int) Math.ceil((startWindow - segment.p2.timestamp) / (double) Constants.windowSize)) * Constants.windowSize;
                        if (segmentsMap.get(time) == null)
                            System.out.println();
                        segmentsMap.get(time).remove(segment);
                    }
                    String redisKey = startWindow + "|" + subTask + "|" + tuple2.f0;
                    jedis.set(redisKey.getBytes(StandardCharsets.UTF_8), Arrays.toByteArray(segments));
                }
            }

            List<Segment> newIndexElems;
            if (adjustInfo.region == null){
                closeThisSubTask();
                return;
            }else {
                newIndexElems = new ArrayList<>(localIndex.rectQuery(adjustInfo.region, false));
            }

            if (adjustInfo.migrateFromST != null){
                for (Integer migrateFromST : adjustInfo.migrateFromST) {
                    List<Segment> segments = null;
                    do{
                        String redisKey = startWindow + "|" + migrateFromST + "|" + subTask;
                        byte[] redisData = jedis.get(redisKey.getBytes(StandardCharsets.UTF_8));
                        if (redisData == null){
                            Thread.sleep(100L);
                        }else {
                            segments = (List<Segment>) Arrays.toObject(redisData);
                            jedis.del(redisKey);
                            newIndexElems.addAll(segments);
                            for (Segment segment : segments) {
                                Long time = startWindow - ((int) Math.ceil((startWindow - segment.p2.timestamp) / (double) Constants.windowSize)) * Constants.windowSize;
                                segmentsMap.computeIfAbsent(time, aLong -> new ArrayList<>()).add(segment);
                            }
                            break;
                        }
                    } while (true);
                }
            }

            localIndex = new RCtree<>(4,1,11, adjustInfo.region,0);
            System.gc();
            ((RCDataNode<Segment>)localIndex.root).elms = newIndexElems;
            localIndex.rebuildRoot(adjustInfo.region);
        }
    }

    private boolean check(short[][] shortsss, int number) {
        int total= 0;
        for (short[] shorts : shortsss) {
            for (short aShort : shorts) {
                total += aShort;
            }
        }
        return total == number;
    }

    boolean segmentssCheck(){
        boolean[] flags = new boolean[]{true};
        segmentsMap.values().forEach(segments -> {
            if (!flags[0])
                return;
            for (Segment segment : segments) {
                if ( !segment.check() ) {
                    flags[0] = false;
                    return;
                }
            }
        });
        return flags[0];
    }

    private void queryResToString(Collector<String> out, Rectangle rect) {
        List<Segment> result = localIndex.rectQuery(rect, true);
        StringBuilder buffer = new StringBuilder();
        buffer.append(rect.toString());
        for (Segment re : result) {
            buffer.append("\t");
            buffer.append(Constants.appendSegment(re));
        }
        out.collect(buffer.toString());
    }


    /**
     * 第一次使用本节点，对成员变量赋初值
     */
    private void openThisSubTask(LocalRegionAdjustInfo adjustInfo) {
        segmentsMap = new HashMap<>();
        localIndex = new RCtree<>(4,1,11, adjustInfo.region.extendLength(Constants.maxSegment),0);
    }

    /**
     * 弃用本处理节点
     */
    private void closeThisSubTask(){
        isFirst = true;
        localIndex = null;
        segmentsMap = null;
        System.gc();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        jedis = new Jedis("localhost");
        subTask = getRuntimeContext().getIndexOfThisSubtask();
        isFirst = true;
        int gridDensity = Constants.gridDensity;
        Rectangle rect = null;
        switch (subTask){
            case 0:
                rect = new GridRectangle(new GridPoint(0,0), new GridPoint(gridDensity/2, gridDensity/2)).toRectangle();
                break;
            case 1:
                rect = new GridRectangle(new GridPoint(0,(gridDensity/2)+1), new GridPoint(gridDensity/2, gridDensity)).toRectangle();
                break;
            case 2:
                rect = new GridRectangle(new GridPoint((gridDensity/2)+1,0), new GridPoint(gridDensity, gridDensity/2)).toRectangle();
                break;
            case 3:
                rect = new GridRectangle(new GridPoint((gridDensity/2)+1,(gridDensity/2)+1), new GridPoint(gridDensity, gridDensity)).toRectangle();
                break;
            default:
                break;
        }
        if (rect != null) {
            isFirst = false;
            openThisSubTask(new LocalRegionAdjustInfo(null, null, rect));
        }
    }
}
