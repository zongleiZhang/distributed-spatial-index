package com.ada.flinkFunction;

import com.ada.GlobalTree.GNode;
import com.ada.GlobalTree.GTree;
import com.ada.common.Constants;
import com.ada.geometry.Point;
import com.ada.geometry.Rectangle;
import com.ada.geometry.Segment;
import com.ada.model.Density;
import com.ada.model.DensityToGlobalElem;
import com.ada.model.GlobalToLocalElem;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class GlobalTreePF extends ProcessWindowFunction<DensityToGlobalElem, GlobalToLocalElem, Integer, TimeWindow> {
    private int subTask;
    private GTree globalTree;
    private Queue<int[][]> densityQueue;
    private int[][] density;
    private int count;

    public  GlobalTreePF(){ }

    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<DensityToGlobalElem> elements,
                        Collector<GlobalToLocalElem> out){
        StringBuilder stringBuffer = new StringBuilder();
        for (int i = 0; i < subTask; i++)
            stringBuffer.append("---------------");
        System.out.println(stringBuffer + "Global--" + subTask + ": " + count / 10L);

        //根据Global Index给输入项分区
        Iterator<DensityToGlobalElem> ite = elements.iterator();
        DensityToGlobalElem element = ite.next();
        int queryCount = 0;
        while (ite.hasNext()){
            Segment segment = (Segment) element;
            List<Integer> leafs = globalTree.searchLeafNodes(segment.rect);
            for (Integer leaf : leafs){
                out.collect(new GlobalToLocalElem(leaf, 1, segment));
            }
            queryCount++;
            if (queryCount%Constants.ratio == 0){
                Point point = segment.rect.getCenter();
                Rectangle queryRect = new Rectangle(point.clone(), point.clone()).extendLength(Constants.radius);
                leafs = globalTree.searchLeafNodes(queryRect);
                for (Integer leaf : leafs) {
                    out.collect(new GlobalToLocalElem(leaf, 2, queryRect));
                }
            }
            element = ite.next();
        }
        Constants.addArrsToArrs(density, ((Density) element).grids, true);

        // 调整Global Index, 然后将调整结果同步到相关的Local Index中。
        if (count%Constants.balanceFre == 0 && subTask == 0){
            densityQueue.add(density);
            Constants.addArrsToArrs(globalTree.density, density, true);
            density = new int[Constants.gridDensity+1][Constants.gridDensity+1];
            if (count > Constants.logicWindow)
                Constants.addArrsToArrs(globalTree.density, densityQueue.remove(), false);

            //调整Global Index
            boolean isAdjust = globalTree.updateTree();

            //Global Index发生了调整
            if (isAdjust){
                //通知Local Index其索引区域发生的变化
                for (Tuple2<Integer, Segment> tuple2 : globalTree.divideRegionInfo) {
                    tuple2.f1.p1.timestamp = count+10;
                    out.collect(tuple2);
                }

                //通知Local Index索引项迁移信息
                for (Segment segment : globalTree.migrateInfo) {
                    out.collect(new Tuple2<>(Constants.divideSubTaskKeyMap.get((int) -segment.p1.timestamp), segment));
                    out.collect(new Tuple2<>(Constants.divideSubTaskKeyMap.get((int) -segment.p2.timestamp), segment));
                }

                //通知被弃用的Local Index
                for (Integer discardLeafID : globalTree.discardLeafIDs)
                    out.collect(new Tuple2<>(Constants.divideSubTaskKeyMap.get(discardLeafID), null));

                globalTree.discardLeafIDs.clear();
                globalTree.migrateInfo.clear();
                globalTree.divideRegionInfo.clear();
            }
        }
        count++;
    }


    private void openWindow() {
        try {
            subTask = getRuntimeContext().getIndexOfThisSubtask();
            densityQueue = new ArrayDeque<>();
            density = new int[Constants.gridDensity+1][Constants.gridDensity+1];
            count = 1;
            globalTree = new GTree();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    @Override
    public void open(Configuration parameters) {
        openWindow();
    }

}
