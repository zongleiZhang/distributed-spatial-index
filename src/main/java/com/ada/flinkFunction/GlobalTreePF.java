package com.ada.flinkFunction;

import com.ada.GlobalTree.GDirNode;
import com.ada.GlobalTree.GTree;
import com.ada.Grid.GridRectangle;
import com.ada.common.Constants;
import com.ada.dispatchElem.*;
import com.ada.trackSimilar.Segment;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class GlobalTreePF extends ProcessWindowFunction<Segment, OneTwoData, Integer, TimeWindow> {
    private int subTask;
    private long startWindow;
    private GTree globalTree;
    private Map<Long, int[][]> densities;
    private int[][] density;

    private transient ValueState<Long> globalHeartbeat;   //心跳信息
    private transient ValueState<GDirNode> globalTreeRoot;
    private transient ValueState<int[][]> densityGrid;

    private QueryableStateClient client = null;
    private JobID jobID = null;

    private long count;

    public  GlobalTreePF(){ }

    @Override
    public void process(Integer tuple, Context context, Iterable<Segment> elements, Collector<OneTwoData> out) throws Exception {
        startWindow = context.window().getStart();
        StringBuilder stringBuffer = new StringBuilder();
        for (int i = 0; i < subTask; i++)
            stringBuffer.append("---------------");
        System.out.println(stringBuffer + "Global--" + subTask + ": " + count / 10L);


        //根据Global Index给输入项分区
        for (Segment segment : elements) {
            List<Integer> leafs = globalTree.searchLeafNodes(GridRectangle.rectangleToGridRectangle(segment.rect));
            for (Integer leaf : leafs)
                out.collect(new Tuple2<>(Constants.divideSubTaskKeyMap.get(leaf), segment));
        }


        if ( (count/10)%Constants.densityFre == 0){
            densities.put(startWindow, globalTree.density.data);
            Constants.addArrsToArrs(density, globalTree.density.data,true);
            globalTree.density.data = new int[Constants.gridDensity+1][Constants.gridDensity+1];
            removeOutdate();
        }

        //subTask 0 从Local Index中收集索引密度信息调整Global Index。
        // 然后将调整结果同步到别的subTask和相关的Local Index中。
        if ( (count/10L)%(Constants.balanceFre) == 0L){
            if (subTask == 0){
                Constants.addArrsToArrs(globalTree.density.data, density, true);
                //从Local Index中收集索引密度信息
                getDivideGrid();

                //调整Global Index
                boolean isAdjust = globalTree.updateTree();

                globalTree.density.data = new int[Constants.gridDensity+1][Constants.gridDensity+1];

                //发生了Global Index的调整，同步Global Index到其他subTask中。
                if (isAdjust){
                    globalTreeRoot.update(globalTree.root);
                    globalHeartbeat.update(count+1);

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
                }else
                    globalHeartbeat.update(count);
                waitSyn(count+1);
            }else {
                densityGrid.update(density);
                globalHeartbeat.update(count);
//                waitDensityGridSyn(count + 1);

                synGlobalTree();
                globalHeartbeat.update(count+1);
            }
        }
        count = (count/10L + 1L)*10L;
    }

    private void removeOutdate() {
        long logicStartTime = startWindow - Constants.logicWindow*Constants.windowSize;
        List<Long> removeKey = new ArrayList<>();
        densities.forEach((key, value) -> {
            if (key < logicStartTime){
                removeKey.add(key);
            }
        });
        for (Long aLong : removeKey) {
            int[][] remove = densities.remove(aLong);
            if ( remove == null)
                throw new IllegalArgumentException("removeOutDateData map error.");
            else
                Constants.addArrsToArrs(density, remove, false);
        }
    }

    private void synGlobalTree(){
        ValueStateDescriptor<Long> globalHeartbeatDescriptor = new ValueStateDescriptor<>(
                "globalHeartbeatDescriptor",
                TypeInformation.of(new TypeHint<Long>() {
                }).createSerializer(new ExecutionConfig()));
        while (true){
            try {
                CompletableFuture<ValueState<Long>> resultFuture =
                        client.getKvState(jobID, "globalHeartbeat", Constants.globalSubTaskKeyMap.get(0), BasicTypeInfo.INT_TYPE_INFO, globalHeartbeatDescriptor);
                Long remoteHeart = resultFuture.join().value();
                if (remoteHeart == count || remoteHeart == count+1 ) {
                    if (remoteHeart == count + 1) {
                        ValueStateDescriptor<GDirNode> globalTreeRootDescriptor = new ValueStateDescriptor<>(
                                "globalTreeRootDescriptor",
                                TypeInformation.of(new TypeHint<GDirNode>() {
                                }).createSerializer(new ExecutionConfig()));
                        CompletableFuture<ValueState<GDirNode>> resultFuture1 =
                                client.getKvState(jobID, "globalTreeRoot", Constants.globalSubTaskKeyMap.get(0), BasicTypeInfo.INT_TYPE_INFO, globalTreeRootDescriptor);
                        globalTree.root = resultFuture1.join().value();
                    }
                    break;
                } else {
                    Thread.sleep(10L);
                }
            }catch (Exception e){
                System.out.println("synGlobalTree error.");
                e.printStackTrace();
            }
        }
    }

    private void waitDensityGridSyn(long time){
        ValueStateDescriptor<Long> globalHeartbeatDescriptor = new ValueStateDescriptor<>(
                "globalHeartbeatDescriptor",
                TypeInformation.of(new TypeHint<Long>() {
                }).createSerializer(new ExecutionConfig()));
        while (true) {
            try {
                CompletableFuture<ValueState<Long>> resultFuture =
                        client.getKvState(jobID, "globalHeartbeat", Constants.globalSubTaskKeyMap.get(0), BasicTypeInfo.INT_TYPE_INFO, globalHeartbeatDescriptor);
                Long remoteHeart = resultFuture.join().value();
                if (remoteHeart == time)
                    break;
                else
                    Thread.sleep(10L);
            }catch (Exception e){
                System.out.println("waitDensityGridSyn error.");
                e.printStackTrace();
            }
        }
    }

    private void waitSyn(long time) throws Exception{
        ValueStateDescriptor<Long> globalHeartbeatDescriptor = new ValueStateDescriptor<>(
                "globalHeartbeatDescriptor",
                TypeInformation.of(new TypeHint<Long>() {
                }).createSerializer(new ExecutionConfig()));
        Map<Integer, Boolean> flags = new HashMap<>();
        flags.put(0,true);
        boolean flag = false;
        while (!flag) {
            for (Integer key : Constants.globalSubTaskKeyMap.keySet() ) {
                try {
                    if (!flags.keySet().contains(key))
                        flags.put(key, false);
                    if (!flags.get(key)) {
                        CompletableFuture<ValueState<Long>> resultFuture =
                                client.getKvState(jobID, "globalHeartbeat", Constants.globalSubTaskKeyMap.get(key), BasicTypeInfo.INT_TYPE_INFO, globalHeartbeatDescriptor);
                        ValueState<Long> res = resultFuture.join();
                        Long remoteHeart = res.value();
                        if ( remoteHeart == time )
                            flags.replace(key, true);
                    }
                } catch (Exception e) {
                    System.out.println("waitSyn error.");
                    e.printStackTrace();
                }
            }
            flag = true;
            for (Boolean value : flags.values()) {
                if (!value) {
                    flag = false;
                    break;
                }
            }
            if (!flag)
                Thread.sleep(10L );
        }
    }

    private void openWindow() {
        try {
            subTask = getRuntimeContext().getIndexOfThisSubtask();
            densities = new HashMap<>();

            density = new int[Constants.gridDensity+1][Constants.gridDensity+1];

            globalHeartbeat.update(0L);
            densityGrid.update(null);
            count = 10L;
            globalTree = new GTree();
//            if (subTask == 0) {
//                globalTree.mainSubtaskInit();
//                for (Tuple2<Integer, Segment> tuple2 : globalTree.divideRegionInfo) {
//                    tuple2.f1.p1.timestamp = count + 10L;
////                    out.collect(tuple2);
//                }
//                globalTree.divideRegionInfo.clear();
//            }
            client = new QueryableStateClient(Constants.QueryStateIP, 9069);
            jobID = JobID.fromHexString(Constants.getJobIDStr());
        }catch (Exception e){
            e.printStackTrace();
        }
    }



    /**
     * 获取本全局索引中全部divideProcess中的索引项密度网格
     */
    private void getDivideGrid() throws InterruptedException {
        long startTime = System.currentTimeMillis();
        ValueStateDescriptor<Long> globalHeartbeatDescriptor = new ValueStateDescriptor<>(
                "globalHeartbeatDescriptor",
                TypeInformation.of(new TypeHint<Long>() {
                }).createSerializer(new ExecutionConfig()));

        ValueStateDescriptor<int[][]> densityGridDescriptor =
                new ValueStateDescriptor<>(
                        "densityGridDescriptor",
                        TypeInformation.of(new TypeHint<int[][]>() {
                        }).createSerializer(new ExecutionConfig()));
        Map<Integer, Boolean> flags = new HashMap<>();
        boolean flag = false;
        flags.put(Constants.globalSubTaskKeyMap.get(0), true);
        while (!flag) {
            for (Integer key : Constants.globalSubTaskKeyMap.values()) {
                try {
                    if (!flags.keySet().contains(key))
                        flags.put(key, false);
                    if (!flags.get(key)) {
                        try {
                            CompletableFuture<ValueState<Long>> resultFuture =
                                    client.getKvState(jobID, "globalHeartbeat", key, BasicTypeInfo.INT_TYPE_INFO, globalHeartbeatDescriptor);
                            ValueState<Long> res = resultFuture.join();
                            long remoteHeartbeat = res.value();
                            if (remoteHeartbeat == count) {
                                CompletableFuture<ValueState<int[][]>> resultFuture1 =
                                        client.getKvState(jobID, "densityGrid", key, BasicTypeInfo.INT_TYPE_INFO, densityGridDescriptor);
                                int[][] res1 = resultFuture1.join().value();
                                Constants.addArrsToArrs( globalTree.density,res1,true);
                                flags.replace(key, true);
                            }
                        }catch (Exception e){
                            System.out.println("getDivideGrid error.");
                            e.printStackTrace();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            flag = true;
            for (Boolean value : flags.values()) {
                if (!value) {
                    flag = false;
                    break;
                }
            }
            if (!flag) {
                Thread.sleep(10L);
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println(endTime - startTime);
    }



    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Long> globalHeartbeatDescriptor =
                new ValueStateDescriptor<>(
                        "globalHeartbeatDescriptor", // the state name
                        TypeInformation.of(new TypeHint<Long>() {})); // default value of the state, if nothing was set
        globalHeartbeatDescriptor.setQueryable("globalHeartbeat");
        globalHeartbeat = getRuntimeContext().getState(globalHeartbeatDescriptor);

        ValueStateDescriptor<GDirNode> globalTreeRootDescriptor =
                new ValueStateDescriptor<>(
                        "globalTreeRootDescriptor", // the state name
                        TypeInformation.of(new TypeHint<GDirNode>() {})); // default value of the state, if nothing was set
        globalTreeRootDescriptor.setQueryable("globalTreeRoot");
        globalTreeRoot = getRuntimeContext().getState(globalTreeRootDescriptor);

        ValueStateDescriptor<int[][]> densityGridDescriptor =
                new ValueStateDescriptor<>(
                        "densityGridDescriptor", // the state name
                        TypeInformation.of(new TypeHint<int[][]>() {})); // default value of the state, if nothing was set
        densityGridDescriptor.setQueryable("densityGrid");
        densityGrid = getRuntimeContext().getState(densityGridDescriptor);

        openWindow();
    }

}
