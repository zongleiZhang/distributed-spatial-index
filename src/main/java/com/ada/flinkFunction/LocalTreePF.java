package com.ada.flinkFunction;

import com.ada.QBSTree.RCDataNode;
import com.ada.geometry.GridRectangle;
import com.ada.QBSTree.RCtree;
import com.ada.common.Constants;
import com.ada.geometry.Point;
import com.ada.geometry.Rectangle;
import com.ada.geometry.Segment;
import com.ada.model.GlobalToLocalElem;
import com.ada.model.LocalRegionAdjustInfo;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class LocalTreePF extends ProcessWindowFunction<GlobalToLocalElem, String, Integer, TimeWindow> {
    private boolean isFirst = true;
    private int subTask;
    private RCtree<Segment> localIndex;
    private Map<Long,List<Segment>> segmentsMap;
    private Jedis jedis = new Jedis("localhost");


    @Override
    public void process(Integer key, Context context, Iterable<GlobalToLocalElem> elements, Collector<String> out) throws Exception {
        long startWindow = context.window().getStart();

        StringBuilder stringBuffer = new StringBuilder();
        stringBuffer.append("                                             ");
        stringBuffer.append("------------------------------");
        for (int i = 0; i < subTask; i++)
            stringBuffer.append("---------------");
        int total = 0;
        for (List<Segment> value : segmentsMap.values())
            total += value.size();
        System.out.println(stringBuffer + "Local--" + subTask + " "+ key + ": " + startWindow + "\t" + total);

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
            openThisSubtask(adjustInfo);
            adjustInfo = null;
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
                        segmentsMap.get(time).remove(segment);
                    }
                    String redisKey = startWindow + "|" + subTask + "|" + tuple2.f0;
                    jedis.set(redisKey.getBytes(StandardCharsets.UTF_8), toByteArray(segments));
                }
            }

            List<Segment> newIndexElems;
            if (adjustInfo.region == null){
                closeThisSubtask();
                return;
            }else {
                newIndexElems = new ArrayList<>(localIndex.rectQuery(adjustInfo.region, false));
            }

            if (adjustInfo.migrateFromST != null){
                for (Integer migrateFromST : adjustInfo.migrateFromST) {
                    List<Segment> segments = null;
                    do{
                        String redisKey = startWindow + "|" + migrateFromST + "|" + subTask;
                        segments = (List<Segment>) toObject(jedis.get(redisKey.getBytes(StandardCharsets.UTF_8)));
                        if (segments == null){
                            Thread.sleep(100L);
                        }else {
                            jedis.del(redisKey);
                            newIndexElems.addAll(segments);
                            for (Segment segment : segments) {
                                Long time = startWindow - ((int) Math.ceil((startWindow - segment.p2.timestamp) / (double) Constants.windowSize)) * Constants.windowSize;
                                segmentsMap.get(time).add(segment);
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

    /**
     * 对象转数组
     */
    private byte[] toByteArray (Object obj) {
        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray ();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return bytes;
    }

    /**
     * 数组转对象
     */
    private Object toObject (byte[] bytes) {
        Object obj = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream (bytes);
            ObjectInputStream ois = new ObjectInputStream (bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return obj;
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
    private void openThisSubtask(LocalRegionAdjustInfo adjustInfo) {
        segmentsMap = new HashMap<>();
        localIndex = new RCtree<>(4,1,11, adjustInfo.region,0);
        subTask = getRuntimeContext().getIndexOfThisSubtask();
    }

    /**
     * 弃用本处理节点
     */
    private void closeThisSubtask(){
        isFirst = true;
        localIndex = null;
        segmentsMap = null;
        System.gc();
    }
}
