package com.ada.flinkFunction.DPIflinkFunction;

import com.ada.GlobalTree.GTree;
import com.ada.Grid.DensityGrid;
import com.ada.Grid.GridPoint;
import com.ada.Grid.GridRectangle;
import com.ada.QBSTree.RCtree;
import com.ada.common.*;
import com.ada.trackSimilar.Point;
import com.ada.trackSimilar.Rectangle;
import com.ada.trackSimilar.TrackPointElem;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestAWF extends ProcessAllWindowFunction<TrackPointElem, String, TimeWindow> {
    private boolean isFirst = true;
    private RCtree<TrackPointElem> localIndex;
    private DensityGrid density;
    private GTree globalTree;

    private Map<Long, List<TrackPointElem>> segmentss;
    private Long startWindow;
    private int count;

    @Override
    public void process(Context context, Iterable<TrackPointElem> elements, Collector<String> out) throws Exception {
        startWindow = context.window().getStart();
        if (isFirst)
            openWindow();
        int tatol = 0;
        for (List<TrackPointElem> value : segmentss.values()) {
            tatol += value.size();
        }
        System.out.println(count + "\t" + tatol + "\t" + localIndex.root.elemNum);

//        testGlocalTree(elements);


//        normalProcess(elements, out);

        count++;
    }

    private void normalProcess(Iterable<TrackPointElem> elements, Collector<String> out) throws Exception {
        long logicStartTime = startWindow - Constants.logicWindow*Constants.windowSize*1000;
        List<Long> removeKey = new ArrayList<>();
        for (Long key : segmentss.keySet()) {
            if (key < logicStartTime)
                removeKey.add(key);
        }
        for (Long aLong : removeKey)
            segmentss.remove(aLong);

        List<Tuple2<TrackPointElem, Rectangle>> queryEdema = new ArrayList<>();
        List<TrackPointElem> indexEdema = new ArrayList<>();
        int flag = 0;
        for (TrackPointElem elem : elements) {
            if (flag == 30){
                flag = 0;
                Rectangle rectangle = (new Rectangle(new Point(elem.data.clone()), new Point(elem.data.clone()))).extendLength(35.0);
                queryEdema.add(new Tuple2<>(elem, rectangle));
            }
            indexEdema.add(elem);
            flag++;

        }
        segmentss.put(startWindow, indexEdema);

        for (Tuple2<TrackPointElem,Rectangle> querySeg : queryEdema) {
            List<TrackPointElem> indexData = new ArrayList<>();
            for (List<TrackPointElem> value : segmentss.values()) {
                for (TrackPointElem segment : value) {
//                    if (querySeg.rect.isIntersection(segment.rect))
                    if (querySeg.f1.isInternal(segment))
                        indexData.add(segment);
                }
            }
            StringBuilder buffer = new StringBuilder();
            buffer.append(Constants.appendTrackPoint(querySeg.f0));
            for (TrackPointElem re : indexData) {
                buffer.append("\t");
                buffer.append(Constants.appendTrackPoint(re));
            }
            out.collect(buffer.toString());
        }
        


//        统计数量
//        long total = 0;
//        for (List<Segment> value : segmentss.values()) {
//            total += value.size();
//        }
//        System.out.println( total0 + "\t\t" + total);

    }

    private void openWindow() {
        isFirst = false;
        globalTree = new GTree();
        globalTree.mainSubtaskInit();
        GridRectangle gridRectangle = new GridRectangle(new GridPoint(0,0), new GridPoint(511,511));
        localIndex = new RCtree<>(5,1,17, gridRectangle.toRectangle().extendToInt(),0, false);
//        density = new DensityGrid(gridRectangle);
        segmentss = new HashMap<>();
        count = 1;
    }


//    private void testGlocalTree(Iterable<Segment> elements) {
//        long logicStartTime = startWindow - Constants.logicWindow*Constants.windowSize;
//        List<Long> removeKey = new ArrayList<>();
//        segmentss.forEach((key, value) -> {
//            if (key < logicStartTime){
//                removeKey.add(key);
//                for (Segment segment : value) {
//                    globalTree.density.alterElemNum(segment,-1);
//                }
//            }
//        });
//        for (Long aLong : removeKey)
//            segmentss.remove(aLong);
//
//        List<Segment> indexEdema = new ArrayList<>();
//        for (Segment segment : elements) {
//            if (segment.center != null) {//查询矩形
//                indexEdema.add(segment);
//                globalTree.density.alterElemNum(segment, 1);
//            }
//        }
//        segmentss.put(startWindow,indexEdema);
//
//        int total0 = 0;
//        int[] total1 = new int[]{0};
//
//        for (int[] datum : globalTree.density.data) {
//            for (int i : datum) {
//                total0 += i;
//            }
//        }
//
//        segmentss.values().forEach(segments -> total1[0] += segments.size());
//
//        if (total0 != total1[0])
//            total0 += 0;
//
//        if ( count%5 == 0 ) {
//            globalTree.updateTree();
//            System.out.print(count + "\t" + (total0/10000) + "\t" + globalTree.leafIDMap.size() + "\t");
//            globalTree.check();
//            globalTree.migrateInfo.clear();
//            globalTree.discardLeafIDs.clear();
//            globalTree.divideRegionInfo.clear();
//        }
//
//    }


    private boolean check() {
        int total= 0;
//        for (int[] shorts : density.data) {
//            for (int aShort : shorts) {
//                total += aShort;
//            }
//        }
        if (total != localIndex.root.elemNum)
            return false;
        else
            return true;
    }

}
