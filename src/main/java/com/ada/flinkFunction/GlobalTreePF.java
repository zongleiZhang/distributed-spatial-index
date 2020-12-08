package com.ada.flinkFunction;

import com.ada.GlobalTree.GDataNode;
import com.ada.GlobalTree.GNode;
import com.ada.GlobalTree.GTree;
import com.ada.common.Constants;
import com.ada.common.collections.Collections;
import com.ada.geometry.Point;
import com.ada.geometry.Rectangle;
import com.ada.geometry.Segment;
import com.ada.model.AdjustLocalRegion;
import com.ada.model.Density;
import com.ada.model.DensityToGlobalElem;
import com.ada.model.GlobalToLocalElem;
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
                for (Integer leafID : leafs) {
                    out.collect(new GlobalToLocalElem(leafID, 2, queryRect));
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
            Map<GNode, GNode> nodeMap = globalTree.updateTree();

            //Global Index发生了调整，通知Local Index迁移数据，重建索引。
            if (!nodeMap.isEmpty() && subTask == 0)
                adjustLocalTasksRegion(nodeMap, out);

            globalTree.discardLeafIDs.clear();
        }
        count++;
    }

    private void adjustLocalTasksRegion(Map<GNode, GNode> nodeMap,
                                        Collector<GlobalToLocalElem> out){
        Map<Integer, AdjustLocalRegion> migrateOutMap = new HashMap<>();
        Map<Integer, AdjustLocalRegion> migrateFromMap = new HashMap<>();
        for (Map.Entry<GNode, GNode> entry : nodeMap.entrySet()) {
            for (GDataNode oldLeaf : entry.getKey().getLeafs()) {
                List<GDataNode> migrateOutLeafs = new ArrayList<>();
                entry.getValue().getIntersectLeafNodes(oldLeaf.region, migrateOutLeafs);
                List<Integer> migrateOutLeafIDs = (List<Integer>) Collections.changeCollectionElem(migrateOutLeafs, node -> node.leafID);
                migrateOutLeafIDs.remove(new Integer(oldLeaf.leafID));
                migrateOutMap.put(oldLeaf.leafID, new AdjustLocalRegion(migrateOutLeafIDs, null, null));
            }
            for (GDataNode newLeaf : entry.getValue().getLeafs()) {
                List<GDataNode> migrateFromLeafs = new ArrayList<>();
                entry.getKey().getIntersectLeafNodes(newLeaf.region, migrateFromLeafs);
                List<Integer> migrateFromLeafIDs = (List<Integer>) Collections.changeCollectionElem(migrateFromLeafs, node -> node.leafID);
                migrateFromLeafIDs.remove(new Integer(newLeaf.leafID));
                migrateFromMap.put(newLeaf.leafID, new AdjustLocalRegion(null, migrateFromLeafIDs, newLeaf.region));
            }
        }
        Iterator<Map.Entry<Integer, AdjustLocalRegion>> ite = migrateOutMap.entrySet().iterator();
        while (ite.hasNext()){
            Map.Entry<Integer, AdjustLocalRegion> entry = ite.next();
            AdjustLocalRegion elem = migrateFromMap.get(entry.getKey());
            if (elem != null){
                elem.setMigrateOutST(entry.getValue().getMigrateOutST());
                ite.remove();
            }
        }
        migrateFromMap.putAll(migrateOutMap);
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
