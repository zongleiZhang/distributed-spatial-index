package com.ada.flinkFunction;

import com.ada.globalTree.GDataNode;
import com.ada.globalTree.GNode;
import com.ada.globalTree.GTree;
import com.ada.common.Constants;
import com.ada.common.collections.Collections;
import com.ada.geometry.Point;
import com.ada.geometry.Rectangle;
import com.ada.geometry.Segment;
import com.ada.model.LocalRegionAdjustInfo;
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
    private int count;

    public  GlobalTreePF(){ }

    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<DensityToGlobalElem> elements,
                        Collector<GlobalToLocalElem> out){
//        StringBuilder stringBuffer = new StringBuilder();
//        for (int i = 0; i < subTask; i++)
//            stringBuffer.append("---------------");
//        System.out.println(stringBuffer + "Global--" + subTask + ": " + count / 10L);

        //第一个窗口需要告知从节点需要负责的索引区域
        if (count == 1 && subTask == 0){
            for (GNode gNode : globalTree.leafIDMap.values()) {
                GDataNode leaf = (GDataNode) gNode;
                out.collect(new GlobalToLocalElem(leaf.leafID, 3, new LocalRegionAdjustInfo(null, null, leaf.region)));
            }
        }

        //根据Global Index给输入项分区
        int queryCount = 0;
        List<int[][]> densities = new ArrayList<>(Constants.globalPartition);
        for (DensityToGlobalElem element : elements) {
            if (element instanceof Segment){
                Segment segment = (Segment) element;
                List<Integer> leafs = globalTree.searchLeafNodes(segment.rect);
                for (Integer leaf : leafs){
                    out.collect(new GlobalToLocalElem(leaf, 1, segment));
                }
                queryCount++;
                if (queryCount%Constants.ratio == 0){
                    Point point = segment.p1;
                    Rectangle queryRect = new Rectangle(point.clone(), point.clone()).extendLength(Constants.radius);
                    leafs = globalTree.searchLeafNodes(queryRect);
                    for (Integer leafID : leafs) {
                        out.collect(new GlobalToLocalElem(leafID, 2, queryRect));
                    }
                }
            }else {
                densities.add(((Density) element).grids);
            }
        }
        for (int i = 1; i < densities.size(); i++)
            Constants.addArrsToArrs(densities.get(0), densities.get(i), false);


        // 调整Global Index, 然后将调整结果同步到相关的Local Index中。
        if (count%Constants.balanceFre == 0){
            densityQueue.add(densities.get(0));
            Constants.addArrsToArrs(globalTree.density, densities.get(0), true);
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
        Map<Integer, LocalRegionAdjustInfo> migrateOutMap = new HashMap<>();
        Map<Integer, LocalRegionAdjustInfo> migrateFromMap = new HashMap<>();
        for (Map.Entry<GNode, GNode> entry : nodeMap.entrySet()) {
            for (GDataNode oldLeaf : entry.getKey().getLeafs()) {
                List<GDataNode> migrateOutLeafs = new ArrayList<>();
                entry.getValue().getIntersectLeafNodes(oldLeaf.region, migrateOutLeafs);
                migrateOutLeafs.removeIf(leaf -> leaf.leafID == oldLeaf.leafID);
                List<Tuple2<Integer, Rectangle>> migrateOutLeafIDs = (List<Tuple2<Integer, Rectangle>>) Collections.changeCollectionElem(migrateOutLeafs, node -> new Tuple2<>(node.leafID,node.region));
                migrateOutMap.put(oldLeaf.leafID, new LocalRegionAdjustInfo(migrateOutLeafIDs, null, null));
            }
            for (GDataNode newLeaf : entry.getValue().getLeafs()) {
                List<GDataNode> migrateFromLeafs = new ArrayList<>();
                entry.getKey().getIntersectLeafNodes(newLeaf.region, migrateFromLeafs);
                List<Integer> migrateFromLeafIDs = (List<Integer>) Collections.changeCollectionElem(migrateFromLeafs, node -> node.leafID);
                migrateFromLeafIDs.remove(new Integer(newLeaf.leafID));
                migrateFromMap.put(newLeaf.leafID, new LocalRegionAdjustInfo(null, migrateFromLeafIDs, newLeaf.region));
            }
        }
        Iterator<Map.Entry<Integer, LocalRegionAdjustInfo>> ite = migrateOutMap.entrySet().iterator();
        while (ite.hasNext()){
            Map.Entry<Integer, LocalRegionAdjustInfo> entry = ite.next();
            LocalRegionAdjustInfo elem = migrateFromMap.get(entry.getKey());
            if (elem != null){
                elem.setMigrateOutST(entry.getValue().getMigrateOutST());
                ite.remove();
            }
        }
        migrateFromMap.putAll(migrateOutMap);
        for (Map.Entry<Integer, LocalRegionAdjustInfo> entry : migrateFromMap.entrySet())
            out.collect(new GlobalToLocalElem(entry.getKey(), 3, entry.getValue()));
    }

    private void openWindow() {
        try {
            subTask = getRuntimeContext().getIndexOfThisSubtask();
            densityQueue = new ArrayDeque<>();
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
