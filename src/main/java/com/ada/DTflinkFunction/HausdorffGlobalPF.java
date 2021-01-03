package com.ada.DTflinkFunction;

import com.ada.globalTree.GDataNode;
import com.ada.globalTree.GNode;
import com.ada.globalTree.GTree;
import com.ada.QBSTree.RCtree;
import com.ada.common.Arrays;
import com.ada.common.Constants;
import com.ada.geometry.*;
import com.ada.model.densityToGlobal.Density;
import com.ada.model.densityToGlobal.DensityToGlobalElem;
import com.ada.model.globalToLocal.GlobalToLocalElem;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;
import java.util.*;

public class HausdorffGlobalPF extends ProcessWindowFunction<DensityToGlobalElem, GlobalToLocalElem, Integer, TimeWindow> {

    private boolean hasInit;
    private int subTask;
    private GTree globalTree;
    private Queue<int[][]> densitiesQue;
    private Queue<RoaringBitmap> tIDsQue;

    private Map<Integer, TrackKeyTID> trackMap;
    private Map<Integer, TrackPoint> singlePointMap;
    private RCtree<Segment> pointIndex;
    private RCtree<TrackKeyTID> pruneIndex;
    private Collector<GlobalToLocalElem> out;
    private int count;

    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<DensityToGlobalElem> elements,
                        Collector<GlobalToLocalElem> out) throws Exception {
        //根据Global Index给输入项分区
        List<Density> densities = new ArrayList<>(Constants.globalPartition);
        for (DensityToGlobalElem element : elements) {
            if (element instanceof TrackPoint){

            }else {
                densities.add((Density) element);
            }
        }
        int[][] result = null;
        if (!densities.isEmpty()){
            result = new int[Constants.gridDensity+1][Constants.gridDensity+1];
            for (Density density : densities) Arrays.addArrsToArrs(result, density.grids, true);
        }



        // 调整Global Index, 然后将调整结果同步到相关的Local Index中。
        if (density != null){
            densityQueue.add(density);
            Arrays.addArrsToArrs(globalTree.density, density, true);
            if (densityQueue.size() > Constants.logicWindow / Constants.balanceFre) {
                int[][] removed = densityQueue.remove();
                Arrays.addArrsToArrs(globalTree.density, removed, false);
            }

            //调整Global Index
            Map<GNode, GNode> nodeMap = globalTree.updateTree();

            //Global Index发生了调整，通知Local Index迁移数据，重建索引。
            if (!nodeMap.isEmpty() && subTask == 0)
                adjustLocalTasksRegion(nodeMap, out);
        }
    }


    private void adjustLocalTasksRegion(Map<GNode, GNode> nodeMap,
                                        Collector<GlobalToLocalElem> out){
        Map<Integer, LocalRegionAdjustInfo> migrateOutMap = new HashMap<>();
        Map<Integer, LocalRegionAdjustInfo> migrateFromMap = new HashMap<>();
        for (Map.Entry<GNode, GNode> entry : nodeMap.entrySet()) {
            List<GDataNode> leafs = new ArrayList<>();
            entry.getKey().getLeafs(leafs);
            for (GDataNode oldLeaf : leafs) {
                List<GDataNode> migrateOutLeafs = new ArrayList<>();
                entry.getValue().getIntersectLeafNodes(oldLeaf.region, migrateOutLeafs);
                migrateOutLeafs.removeIf(leaf -> leaf.leafID == oldLeaf.leafID);
                List<Tuple2<Integer, Rectangle>> migrateOutLeafIDs = (List<Tuple2<Integer, Rectangle>>) com.ada.common.collections.Collections.changeCollectionElem(migrateOutLeafs, node -> new Tuple2<>(node.leafID,node.region));
                migrateOutMap.put(oldLeaf.leafID, new LocalRegionAdjustInfo(migrateOutLeafIDs, null, null));
            }
            leafs.clear();
            entry.getValue().getLeafs(leafs);
            for (GDataNode newLeaf : leafs) {
                List<GDataNode> migrateFromLeafs = new ArrayList<>();
                entry.getKey().getIntersectLeafNodes(newLeaf.region, migrateFromLeafs);
                List<Integer> migrateFromLeafIDs = (List<Integer>) com.ada.common.collections.Collections.changeCollectionElem(migrateFromLeafs, node -> node.leafID);
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

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        subTask = getRuntimeContext().getIndexOfThisSubtask();
        densitiesQue = new ArrayDeque<>();
        globalTree = new GTree();
        hasInit = false;
        tIDsQue = new ArrayDeque<>();
        trackMap = new HashMap<>();
        singlePointMap = new HashMap<>();
        pointIndex = new RCtree<>();
        private RCtree<TrackKeyTID> pruneIndex;
        private Collector<GlobalToLocalElem> out;
        private int count;
    }
}
