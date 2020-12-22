package com.ada.flinkFunction;

import com.ada.QBSTree.RCDataNode;
import com.ada.QBSTree.RCtree;
import com.ada.common.Arrays;
import com.ada.common.Constants;
import com.ada.common.collections.Collections;
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

import java.nio.charset.StandardCharsets;
import java.util.*;

public class LocalTreePF extends ProcessWindowFunction<GlobalToLocalElem, String, Integer, TimeWindow> {
    private boolean isFirst;
    private long startWindow;
    private int subTask;
    private RCtree<Segment> localIndex;
    private Map<Long,List<Segment>> segmentsMap;
    private Jedis jedis;


    @Override
    public void process(Integer key, Context context, Iterable<GlobalToLocalElem> elements, Collector<String> out) throws Exception {
        startWindow = context.window().getStart();

        if (subTask == 1 && startWindow == 1477931640000L)
            System.out.println();

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

        //初始化成员变量
        if (isFirst) {
            assert adjustInfo != null;
            openThisSubTask(adjustInfo);
            isFirst = false;
        }

        //插入和查询
        for (Segment segment : indexItems) localIndex.insert(segment);
        segmentsMap.put(startWindow, indexItems);
        for (Rectangle rectangle : queryItems) queryResToString(out, rectangle);

        check();
        //从索引中移除过时数据
        removeOutDate();

        //需要数据迁移
        if (adjustInfo != null) migrateAndRebuild(adjustInfo);
        check();
    }

    /**
     * 从索引中移除过时数据
     */
    private void removeOutDate() {
        long logicStartTime = startWindow - Constants.logicWindow*Constants.windowSize;
        Iterator<Map.Entry<Long, List<Segment>>> ite = segmentsMap.entrySet().iterator();
        while (ite.hasNext()){
            Map.Entry<Long, List<Segment>> entry = ite.next();
            if (entry.getKey() < logicStartTime){
                ite.remove();
                for (Segment segment : entry.getValue())
                    localIndex.delete(segment);
            }
        }
    }

    /**
     * 数据迁移并重建索引
     */
    private void migrateAndRebuild(LocalRegionAdjustInfo adjustInfo) throws Exception {
        //将需要迁出的数据添加到Redis中
        if (adjustInfo.migrateOutST != null){
            for (Tuple2<Integer, Rectangle> tuple2 : adjustInfo.migrateOutST) {
                List<Segment> segments = localIndex.rectQuery(tuple2.f1, false);
                String redisKey = "LocalTreePF" + startWindow + "|" + subTask + "|" + tuple2.f0;
                jedis.set(redisKey.getBytes(StandardCharsets.UTF_8), Arrays.toByteArray(segments));
            }
        }

        if (adjustInfo.region == null){
            closeThisSubTask();
            return;
        }

        List<Segment> newIndexElems = localIndex.rectQuery(adjustInfo.region, false);

        if (adjustInfo.migrateFromST != null){
            for (Integer migrateFromST : adjustInfo.migrateFromST) {
                List<Segment> segments;
                do{
                    String redisKey = "LocalTreePF" + startWindow + "|" + migrateFromST + "|" + subTask;
                    byte[] redisData = jedis.get(redisKey.getBytes(StandardCharsets.UTF_8));
                    if (redisData == null){
                        Thread.sleep(100L);
                    }else {
                        segments = (List<Segment>) Arrays.toObject(redisData);
                        jedis.del(redisKey);
                        newIndexElems.addAll(segments);
                        break;
                    }
                } while (true);
            }
        }
        if (subTask == 1 && startWindow == 1477931520000L)
            System.out.println();

        segmentsMap = new HashMap<>(21);
        localIndex = new RCtree<>(4,1,11, adjustInfo.region.extendLength(Constants.maxSegment),0, newIndexElems);
        System.gc();
        for (Segment segment : newIndexElems) {
            Long time = startWindow - ((int) Math.ceil((startWindow - segment.p2.timestamp) / (double) Constants.windowSize)) * Constants.windowSize;
            segmentsMap.computeIfAbsent(time, aLong -> new ArrayList<>()).add(segment);
        }
    }

    boolean check(){
        localIndex.check();
        List<Segment> treeSegments = localIndex.getAllElems();
        Set<Segment> mapSegments = new HashSet<>(treeSegments.size());
        for (List<Segment> value : segmentsMap.values()) mapSegments.addAll(value);
        if (treeSegments.size() != mapSegments.size())
            System.out.print("");
        for (Segment treeSegment : treeSegments) {
            if (!mapSegments.remove(treeSegment))
                System.out.print("");
        }
        if (!mapSegments.isEmpty())
            System.out.print("");
        for (Map.Entry<Long, List<Segment>> entry : segmentsMap.entrySet()) {
            for (Segment segment : entry.getValue()) {
                Long time = startWindow - ((int) Math.ceil((startWindow - segment.p2.timestamp) / (double) Constants.windowSize)) * Constants.windowSize;
                if (!entry.getKey().equals(time))
                    System.out.print("");
            }
        }
        return true;
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
