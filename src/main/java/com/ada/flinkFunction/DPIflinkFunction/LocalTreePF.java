package com.ada.flinkFunction.DPIflinkFunction;

import com.ada.Grid.GridPoint;
import com.ada.Grid.GridRectangle;
import com.ada.QBSTree.ElemRoot;
import com.ada.QBSTree.RCtree;
import com.ada.common.Constants;
import com.ada.trackSimilar.Point;
import com.ada.trackSimilar.Rectangle;
import com.ada.trackSimilar.Segment;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class LocalTreePF extends ProcessWindowFunction<Tuple2<Integer, Segment>, String, Tuple, TimeWindow> {
    private boolean isFirst = true;
    private boolean isClose;
    private int subTask;
    private RCtree<Segment> localIndex;
//    private DensityGrid density;
    private Map<Long,List<Segment>> segmentss;
    private long count;
    private long startWindow;


//    private transient ValueState<Long> densityHeartbeat;   //心跳信息
//    private transient ValueState<Tuple2<short[][], Integer>> densityGrid;
    private transient ValueState<Long> divideHeartbeat;   //心跳信息
    private transient ListState<Segment> migrateOutData;
    private QueryableStateClient client = null;
    private JobID jobID = null;

    private List<Segment> indexData;           //索引项信息
    private List<Integer> migrateFrom;         //索引项迁入信息
    private Map<Integer, Segment> migrateTo; //索引项迁出信息
    private GridRectangle[] newRootRectangle;  //Local Index重建信息


    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple2<Integer, Segment>> elements, Collector<String> out) throws Exception {
        startWindow = context.window().getStart();
        if (isFirst)
            openThisSubtask();

        if (subTask == 4 && count == 310)
            subTask += 0;

        StringBuilder stringBuffer = new StringBuilder();
        stringBuffer.append("               ");
        stringBuffer.append("               ");
        stringBuffer.append("               ");
        stringBuffer.append("---------------");
        stringBuffer.append("---------------");
        for (int i = 0; i < subTask; i++)
            stringBuffer.append("---------------");
        int total = 0;
        for (List<Segment> value : segmentss.values())
            total += value.size();
        System.out.println(stringBuffer + "Local--" + subTask + " "+ ((Tuple1) tuple).f0 + ": " + count/10L + "\t" + total);

        //将输入数据分类成索引项信息indexData、索引项迁入信息migrateFrom、
        // 索引项迁出信息migrateTo、Local Index重建信息newRootRectangle。
        classificationData(elements);

        //初次使用sunTask创建Local Index和density
        if (isFirst)
            createTreeAndGrid();

//        segmentssCheck();
        //从索引中移除过时数据
        removeOutDateData();

        //插入新的索引项，并执行查询操作
        if (indexData.size() > 0) {
            processIndexData(out);
        }

        //将迁出的索引项添加到migrateOutData中
        if (migrateTo.size() > 0){
            for (Segment value : migrateTo.values()) {
                List<Segment> list = new ArrayList<>();
                for (Segment elem : localIndex.<Segment>rectQuery(value.rect, true)) {
                    Segment segment2 = new Segment(elem.p1, elem.p2);
                    segment2.leaf = null;
                    list.add(segment2);
                }
                migrateOutData.addAll(list);
            }
            divideHeartbeat.update(count);
        }

        //重建本subTask中的Local Index
        if (!isFirst && newRootRectangle[0] != null)
            rebuildLocalIndex();
        else
            isFirst = false;

        //迁入远程索引项
        if (migrateFrom.size() > 0){
            getMigrateData(migrateFrom);
            divideHeartbeat.update(count+1);
        }

        //等待migrateOutData中的数据迁移完成，将migrateOutData中数据清空
        if (!migrateTo.isEmpty()){
            waitMigrate(migrateTo.keySet());
            migrateOutData.clear();
        }



        //更新densityGrid的索引密度信息
//        if ( (count/10L)%(Constants.densityFre/3) == 0 ){
//            short[][] shortss = new short[density.data.length][];
//            for (int i = 0; i < density.data.length; i++)
//                shortss[i] = density.data[i].clone();
//            densityGrid.update(new Tuple2<>(shortss, localIndex.root.elemNum));
//            densityHeartbeat.update(count);
//        }

        //弃用本subTask，清理成员变量
        if (isClose)
            closeThisSubtask();

        closeWindow();
    }

    private boolean check(short[][] shortsss, int number) {
        int total= 0;
        for (short[] shorts : shortsss) {
            for (short aShort : shorts) {
                total += aShort;
            }
        }
        if (total != number)
            return false;
        else
            return true;
    }

    boolean segmentssCheck(){
        boolean[] flags = new boolean[]{true};
        segmentss.forEach((aLong, segments) -> {
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

    /**
     * 本次窗口计算结束，清空分类后的输入数据
     */
    private void closeWindow() {
        indexData.clear();
        migrateFrom.clear();
        migrateTo.clear();
        newRootRectangle[0] = null;
        count = (count/10L + 1L)*10L;
    }

    /**
     * 重新建立本节点局部索引
     */
    private void rebuildLocalIndex() {
        createTreeAndGrid();
        Rectangle rootRectangle = newRootRectangle[0].toRectangle().extendToInt();// density.root.getRegion().clone().extendToInt();
        Map<Long, List<Segment>> newSegmentss = new HashMap<>();
        segmentss.forEach((timestamp, segments) -> {
            List<Segment> newSegments = new ArrayList<>();
            for (Segment oldSegment : segments) {
                if (oldSegment.rect.isIntersection(rootRectangle)) {
                    newSegments.add(oldSegment);
                    localIndex.insert(oldSegment);
//                    density.alterElemNum(oldSegment,true);
                }
            }
            newSegmentss.put(timestamp,newSegments);
        });
        segmentss = newSegmentss;
    }

    /**
     * 处理索引数据
     * @param out 窗口的输出
     */
    private void processIndexData(Collector<String> out) {
        List<Segment> indexElems = new ArrayList<>();
        List<Segment> queryElems = new ArrayList<>();
        for (Segment segment : indexData) {
            if (segment.data == null){//查询矩形
                queryElems.add(segment);
            }else { //插入轨迹段
                indexElems.add(segment);
                localIndex.insert(segment);
//                density.alterElemNum(segment,true);
            }
        }
        segmentss.put(startWindow, indexElems);
        for (Segment querySeg : queryElems)
            queryResToString(out, querySeg, localIndex);
    }

    static <T extends ElemRoot> void queryResToString(Collector<String> out, Segment querySeg, RCtree<T> localIndex) {
        List<Segment> indexData = localIndex.rectQuery(querySeg.rect, true);
        StringBuilder buffer = new StringBuilder();
        buffer.append(Constants.appendSegment(querySeg));
        buffer.append(" ").append(querySeg.p1.timestamp);
        for (Segment re : indexData) {
            buffer.append("\t");
            buffer.append(Constants.appendSegment(re));
        }
        out.collect(buffer.toString());
    }

    /**
     * 将输入数据进行分类
     * @param elements 输入数据
     */
    private void classificationData(Iterable<Tuple2<Integer, Segment>> elements) {
        for (Tuple2<Integer, Segment> tuple2 : elements) {
            Segment segment = tuple2.f1;
            if (segment != null){
                if (segment.p1.TID > 0){ //正常轨迹段
                    indexData.add(segment);
                }else if (segment.p1.TID == -2){ //索引项迁移信息
                    int migrateOutID =  (int) -segment.p1.timestamp;
                    if (migrateOutID == subTask) {
                        migrateTo.put((int) -segment.p2.timestamp, segment);
                    }else {
                        migrateFrom.add(migrateOutID);
                    }
                }else { //Local Index重建信息
                    count = segment.p1.timestamp;
                    newRootRectangle[0] =  new GridRectangle(new GridPoint((int) segment.p1.data[0], (int)segment.p1.data[1]),
                            new GridPoint((int) segment.p2.data[0], (int) segment.p2.data[1]));
                }
            }else { //弃用本处理节点
                isClose = true;
            }
        }
    }


    /**
     * 第一次使用本节点，对成员变量赋初值
     */
    private void openThisSubtask() throws IOException {
        isClose = false;
        segmentss = new HashMap<>();
        localIndex = null;
//        density = null;
        subTask = getRuntimeContext().getIndexOfThisSubtask();
        client = new QueryableStateClient(Constants.QueryStateIP, 9069);
        jobID = JobID.fromHexString(Constants.getJobIDStr());
        count = 20;
        indexData = new ArrayList<>();
        migrateFrom = new ArrayList<>();
        migrateTo = new HashMap<>();
        newRootRectangle = new GridRectangle[1];
//        densityHeartbeat.update(0L);
        divideHeartbeat.update(0L);
    }

    /**
     * 弃用本处理节点
     */
    private void closeThisSubtask() throws IOException {
        isFirst = true;
        isClose = false;
        subTask = -1;
        localIndex = null;
//        density = null;
        segmentss = null;
        client = null;
        jobID = null;
//        densityGrid.update(null);
    }

    /**
     * 清除过时数据
     */
    private void removeOutDateData(){
        long logicStartTime = startWindow - Constants.logicWindow*Constants.windowSize;
        List<Long> removeKey = new ArrayList<>();
        int[] countt = new int[]{0};
        segmentss.forEach((key, value) -> {
            if (key < logicStartTime){
                removeKey.add(key);
                for (Segment segment : value) {
                    localIndex.delete(segment);
//                    density.alterElemNum(segment,false);
                    countt[0]++;
                }
            }
        });
        for (Long aLong : removeKey) {
            if ( segmentss.remove(aLong) == null)
                throw new IllegalArgumentException("removeOutDateData map error.");
        }
    }

    /**
     * 等到远程节点获取本节点的迁移出的数据
     * @param migrateTo 远程节点集合
     */
    private void waitMigrate(Set<Integer> migrateTo) throws Exception{
        ValueStateDescriptor<Long> divideHeartbeatDescriptor = new ValueStateDescriptor<>(
                "divideHeartbeatDescriptor",
                TypeInformation.of(new TypeHint<Long>() {
                }).createSerializer(new ExecutionConfig()));
        Map<Integer, Boolean> flags = new HashMap<>();
        boolean flag = false;
        while (!flag) {
            for (Integer key : migrateTo) {
                if (!flags.keySet().contains(key))
                    flags.put(key, false);
                if (!flags.get(key)) {
                    try {
                        CompletableFuture<ValueState<Long>> resultFuture =
                                client.getKvState(jobID, "divideHeartbeat", Constants.divideSubTaskKayMap.get(key),
                                        BasicTypeInfo.INT_TYPE_INFO, divideHeartbeatDescriptor);
                        Long remoteHeart = resultFuture.join().value();
                        if (remoteHeart == count + 1)
                            flags.replace(key, true);
                    }catch (Exception e){
                        System.out.println("fake waitMigrate error.");
//                        e.printStackTrace();
                    }
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

    /**
     * 获取远程节点中的迁移信息
     */
    private void getMigrateData(List<Integer> migrateIns) throws Exception{
        ValueStateDescriptor<Long> divideHeartbeatDescriptor = new ValueStateDescriptor<>(
                "divideHeartbeatDescriptor",
                TypeInformation.of(new TypeHint<Long>() {
                }).createSerializer(new ExecutionConfig()));
        Map<Integer, Boolean> flags = new HashMap<>();
        boolean flag = false;
        if (newRootRectangle[0] == null)
            System.out.print("");
        Rectangle region = newRootRectangle[0].toRectangle().extendToInt();//density.root.getRegion().extendToInt();
        while (!flag) {
            for (Integer key : migrateIns) {
                if (!flags.keySet().contains(key))
                    flags.put(key, false);
                if (!flags.get(key)) {
                    try {
                        CompletableFuture<ValueState<Long>> resultFuture =
                                client.getKvState(jobID, "divideHeartbeat", Constants.divideSubTaskKayMap.get(key),
                                        BasicTypeInfo.INT_TYPE_INFO, divideHeartbeatDescriptor);
                        Long remoteHeart = resultFuture.join().value();
                        if (remoteHeart >= count) {
                            ListStateDescriptor<Segment> migrateOutDataDescriptor = new ListStateDescriptor<>(
                                    "migrateOutDataDescriptor",
                                    TypeInformation.of(new TypeHint<Segment>() {
                                    }).createSerializer(new ExecutionConfig()));
                            CompletableFuture<ListState<Segment>> resultFuture1 =
                                    client.getKvState(jobID, "migrateOutData", Constants.divideSubTaskKayMap.get(key),
                                            BasicTypeInfo.INT_TYPE_INFO, migrateOutDataDescriptor);
                            ListState<Segment> listState = resultFuture1.join();
                            listState.get().forEach(segment -> {
                                if (region.isIntersection(segment.rect)) {
                                    long timeStamp = segment.p2.timestamp;
                                    long time = startWindow - ((int) Math.ceil((startWindow - timeStamp) / (double) Constants.windowSize)) * Constants.windowSize;
                                    List<Segment> segments = segmentss.get(time);
                                    if (segments == null) {
                                        segments = new ArrayList<>();
                                        segments.add(segment);
                                        segmentss.put(time, segments);
                                    } else {
                                        if (segments.contains(segment))
                                            return;
                                        else
                                            segments.add(segment);
                                    }
                                    localIndex.insert(segment);
//                                    density.alterElemNum(segment, true);
                                }
                            });
                            flags.replace(key, true);
                        }
                    }catch (Exception e){
                        System.out.println("getMigrateData error.");
                        e.printStackTrace();
                    }
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

    /**
     * 创建空的Local Index
     */
    private void createTreeAndGrid() {
//        density = new DensityGrid(gridRectangle);
        Rectangle rectangle = newRootRectangle[0].toRectangle().extendToInt();//density.root.getRegion();
        rectangle = new Rectangle(new Point(rectangle.low.data[0]-600.0, rectangle.low.data[1]-600.0),
                new Point(rectangle.high.data[0]+600.0, rectangle.high.data[1]+600.0));
        localIndex = new RCtree<>(10,1,17, rectangle,0, false);
    }

    @Override
    public void open(Configuration parameters) {
//        ValueStateDescriptor<Long> densityHeartbeatDescriptor =
//                new ValueStateDescriptor<>(
//                        "densityHeartbeatDescriptor", // the state name
//                        TypeInformation.of(new TypeHint<Long>() {})); // default value of the state, if nothing was set
//        densityHeartbeatDescriptor.setQueryable("densityHeartbeat");
//        densityHeartbeat = getRuntimeContext().getState(densityHeartbeatDescriptor);

//        ValueStateDescriptor<Tuple2<short[][], Integer>> densityGridDescriptor =
//                new ValueStateDescriptor<>(
//                        "densityGridDescriptor", // the state name
//                        TypeInformation.of(new TypeHint<Tuple2<short[][], Integer>>() {})); // default value of the state, if nothing was set
//        densityGridDescriptor.setQueryable("densityGrid");
//        densityGrid = getRuntimeContext().getState(densityGridDescriptor);


        ValueStateDescriptor<Long> divideHeartbeatDescriptor =
                new ValueStateDescriptor<>(
                        "divideHeartbeatDescriptor", // the state name
                        TypeInformation.of(new TypeHint<Long>() {})); // default value of the state, if nothing was set
        divideHeartbeatDescriptor.setQueryable("divideHeartbeat");
        divideHeartbeat = getRuntimeContext().getState(divideHeartbeatDescriptor);
        ListStateDescriptor<Segment> migrateOutDataDescriptor =
                new ListStateDescriptor<>(
                        "migrateOutDataDescriptor",
                        TypeInformation.of(new TypeHint<Segment>() {})
                ); // default value of the state, if nothing was set
        migrateOutDataDescriptor.setQueryable("migrateOutData");
        migrateOutData = getRuntimeContext().getListState(migrateOutDataDescriptor);
    }

}
