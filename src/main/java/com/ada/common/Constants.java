package com.ada.common;

import com.ada.geometry.*;
import com.ada.globalTree.GDataNode;
import com.ada.globalTree.GTree;

import java.io.FileInputStream;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.*;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.assignKeyToParallelOperator;

public class Constants implements Serializable {

    /**
     * 定义Double类型的零
     */
    public final static double zero = 0.00001;

    public final static DecimalFormat df = new DecimalFormat("#.00000");

    public static int inputPartition;

    public static int densityPartition;

    /**
     * 全局索引的并行度
     */
    public static int globalPartition;

    /**
     * 本地索引的并行度
     */
    public static int dividePartition;


    public static int keyTIDPartition;

    /**
     * subTask: globalSubTask
     * value: key
     */
    public static Map<Integer,Integer> globalSubTaskKeyMap = new HashMap<>();

    /**
     * subTask: divideSubTask
     * value: key
     */
    public static Map<Integer,Integer> divideSubTaskKeyMap = new HashMap<>();

    /**
     * 密度统计的频度
     */
    public static int densityFre;

    /**
     * 全局索引做动态负载均衡的频度
     */
    public static int balanceFre;

    /**
     * 网格密度
     */
    public final static int gridDensity = 511;

    public static int topK;

    public static int t;

    public static int KNum;

    public static double extend;

    public static long windowSize;

    public static int logicWindow;

    public static Rectangle globalRegion = new Rectangle(new Point(0.0,0.0), new Point(8626.0,8872.0));

    public static double extendToEnoughBig = (globalRegion.high.data[0] - globalRegion.low.data[0])*1.5;

    static {
        try {
            Properties pro = new Properties();
            FileInputStream in = new FileInputStream("conf.properties");
            pro.load(in);
            in.close();

            inputPartition = Integer.parseInt(pro.getProperty("inputPartition"));
            densityPartition = Integer.parseInt(pro.getProperty("densityPartition"));
            globalPartition = Integer.parseInt(pro.getProperty("globalPartition"));
            dividePartition = Integer.parseInt(pro.getProperty("dividePartition"));
            keyTIDPartition = Integer.parseInt(pro.getProperty("keyTIDPartition"));
            densityFre = Integer.parseInt(pro.getProperty("densityFre"));
            GTree.globalLowBound = Integer.parseInt(pro.getProperty("globalLowBound"));
            windowSize = Integer.parseInt(pro.getProperty("windowSize"));
            logicWindow = Integer.parseInt(pro.getProperty("logicWindow"));
            topK = Integer.parseInt(pro.getProperty("topK"));
            KNum = Integer.parseInt(pro.getProperty("KNum"));
            extend = Double.parseDouble(pro.getProperty("extend"));
            t = Integer.parseInt(pro.getProperty("t"));
        }catch (Exception e){
            e.printStackTrace();
        }
        int maxParallelism = 128;
//        int maxParallelism = 256;
        Set<Integer> usedSubtask = new HashSet<>();
        for (int i = 0; i < 1000000; i++) {
            Integer subTask = assignKeyToParallelOperator(i, maxParallelism, globalPartition);
            if (!usedSubtask.contains(subTask)) {
                usedSubtask.add(subTask);
                globalSubTaskKeyMap.put(subTask, i);
                if (usedSubtask.size() == globalPartition)
                    break;
            }
        }
        usedSubtask.clear();

        maxParallelism = 128;
//        maxParallelism = 256;
        for (int i = 0; i < 1000000; i++) {
            Integer subTask = assignKeyToParallelOperator(i, maxParallelism, dividePartition);
            if (!usedSubtask.contains(subTask)) {
                usedSubtask.add(subTask);
                divideSubTaskKeyMap.put(subTask, i);
                if (usedSubtask.size() == dividePartition)
                    break;
            }
        }
    }

    public static Set<Integer> getTIDs(Collection os){
        Set<Integer> TIDs = new HashSet<>();
        for (Object o : os) {
            TrackInfo info = (TrackInfo) o;
            TIDs.add(info.obtainTID());
        }
        return TIDs;
    }


    /**
     * 向轨迹track中添加一个新的候选轨迹comparedTrack
     */
    public static void addTrackCandidate(TrackHauOne track, TrackHauOne comparedTrack) {
        SimilarState state = comparedTrack.getSimilarState(track.trajectory.TID);
        if (state == null) {
            state = Hausdorff.getHausdorff(track.trajectory, comparedTrack.trajectory);
            track.putRelatedInfo(state);
            comparedTrack.putRelatedInfo(state);
        }
        track.candidateInfo.add(comparedTrack.trajectory.TID);
    }

    /**
     * 用指定的阈值threshold计算轨迹的裁剪域。
     */
    public static Rectangle getPruningRegion(Trajectory track, double threshold){
        Rectangle rectangle = null;
        boolean flag = true;
        for (Segment s : track.elms) {
            if (flag){
                flag = false;
                rectangle = s.rect.clone();
            }else {
                rectangle = rectangle.getUnionRectangle(s.rect);
            }
        }
        assert rectangle != null;
        return rectangle.extendLength(threshold);
    }

    /**
     * 缩减轨迹ID集合的元素数到 Constants.k*Constants.c 大小
     * @param selectedTIDs 被缩减的集合
     */
    public static void cutTIDs(Set<Integer> selectedTIDs) {
        Random random = new Random(12306);
        while (selectedTIDs.size() > topK*KNum){
            for (Iterator<Integer> iterator = selectedTIDs.iterator(); iterator.hasNext();){
                iterator.next();
                if (selectedTIDs.size() > topK*KNum){
                    if (random.nextInt()%4 == 0)
                        iterator.remove();
                }else {
                    break;
                }
            }
        }
    }

    public static String appendSegment(Segment queryElem) {
        DecimalFormat df = new DecimalFormat("#.0000");
        return queryElem.getTID() + " " +
                queryElem.p2.timestamp + " " +
                df.format(queryElem.rect.low.data[0]) + " " +
                df.format(queryElem.rect.low.data[1]) + " " +
                df.format(queryElem.rect.high.data[0]) + " " +
                df.format(queryElem.rect.high.data[1]);
    }

    public static String appendTrackPoint(TrackPointElem elem) {
        DecimalFormat df = new DecimalFormat("#.0000");
        return elem.getTID() + " " +
                elem.timestamp + " " +
                df.format(elem.data[0]) + " " +
                df.format(elem.data[1]);
    }


    public static boolean isEqual(double a, double b){
        return Math.abs(a - b) < zero;
    }


    public static boolean rectangleEqual(Rectangle curRectangle, Rectangle orgRectangle) {
        if (curRectangle == null && orgRectangle == null)
            return true;
        else if (curRectangle == null || orgRectangle == null)
            return false;
        else
            return curRectangle.low.equals(orgRectangle.low) &&
                    curRectangle.high.equals(orgRectangle.high);
    }

    public static boolean gridRectangleEquals(GridRectangle curRectangle, GridRectangle orgRectangle) {
        if (curRectangle == null && orgRectangle == null)
            return true;
        else if (curRectangle == null || orgRectangle == null)
            return false;
        else
            return curRectangle.equals(orgRectangle);
    }



    public static <T extends Comparable<? super T>> List<T> collectDis(List<T> list) {
        List<T> res = new ArrayList<>();
        Collections.sort(list);
        int size = list.size()-1;
        res.add(list.get(0));
        res.add(list.get(size/10));
        res.add(list.get(size/2));
        res.add(list.get(9*size/10));
        res.add(list.get(size));
        return res;
    }

    public static boolean collectionsEqual(Collection<GDataNode> collection1, Collection<GDataNode> collection2) {
        Set<GDataNode> set1 = new HashSet<>(collection1);
        Set<GDataNode> set2 = new HashSet<>(collection2);
        set1.removeAll(collection2);
        set2.removeAll(collection1);
        if (!set1.isEmpty())
            return false;
        if (!set2.isEmpty())
            return false;
        return true;
    }

    public static void rectangleToInts(Rectangle rectangle, int[] low, int[] high){
        low[0] = (int) Math.round(rectangle.low.data[0]*10000);
        low[1] = (int) Math.round(rectangle.low.data[1]*10000);
        high[0] = (int) Math.round(rectangle.high.data[0]*10000);
        high[1] = (int) Math.round(rectangle.high.data[1]*10000);

    }

    /**
     * 返回rect0边长缩小多少后，能不与rect1相交。
     */
    public static double countShrinkBound(Rectangle rect0, Rectangle rect1) {
        if (!rect0.isIntersection(rect1))
            throw new IllegalArgumentException("count Shrink Bound error.");
        int[] r0_low = new int[2];
        int[] r0_high = new int[2];
        int[] r1_low = new int[2];
        int[] r1_high = new int[2];
        rectangleToInts(rect0, r0_low, r0_high);
        rectangleToInts(rect1, r1_low, r1_high);
        List<Integer> ins = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            ins.add(r1_high[i] - r0_low[i]);
            ins.add(r0_high[i] - r1_low[i]);
        }
        return Collections.min(ins)/10000.0;
    }

    /**
     * 返回rect0边长扩大多少后，能与rect1相交。
     */
    public static double countEnlargeBound(Rectangle rect0, Rectangle rect1) {
        if (rect0.isIntersection(rect1))
            throw new IllegalArgumentException("count Enlarge Bound error.");
        int[] r0_low = new int[2];
        int[] r0_high = new int[2];
        int[] r1_low = new int[2];
        int[] r1_high = new int[2];
        rectangleToInts(rect0, r0_low, r0_high);
        rectangleToInts(rect1, r1_low, r1_high);
        List<Integer> ins = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            if ( r0_low[i] > r1_high[i] )
                ins.add(r0_low[i] - r1_high[i]);
            if( r0_high[i] < r1_low[i] )
                ins.add( r1_low[i] - r0_high[i] );
        }
        return Collections.max(ins)/10000.0;
    }

    /**
     * 返回rect0边长扩大多少后，能超出rect1的包围。
     */
    public static double countEnlargeOutBound(Rectangle rect0, Rectangle rect1) {
        if (!rect1.isInternal(rect0))
            throw new IllegalArgumentException("count Enlarge Bound error.");
        int[] r0_low = new int[2];
        int[] r0_high = new int[2];
        int[] r1_low = new int[2];
        int[] r1_high = new int[2];
        rectangleToInts(rect1, r0_low, r0_high);
        rectangleToInts(rect0, r1_low, r1_high);
        List<Integer> ins = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            ins.add(r1_low[i] - r0_low[i]);
            ins.add( r0_high[i] - r1_high[i] );
        }
        return Collections.min(ins)/10000.0;
    }

    public static int getStateAnoTID(SimilarState state, int TID){
        int comparedTID;
        if (state.comparingTID == TID)
            comparedTID = state.comparedTID;
        else
            comparedTID = state.comparingTID;
        return comparedTID;
    }

    public static <T> T getElem(Collection<T> collection, Judge<T> judge){
        for (T t : collection) {
            if (judge.accept(t))
                return t;
        }
        return null;
    }

    public static <T> T removeElem(Collection<T> collection, Judge<T> judge){
        for (Iterator<T> ite = collection.iterator(); ite.hasNext();){
            T t = ite.next();
            if (judge.accept(t)) {
                ite.remove();
                return t;
            }
        }
        return null;
    }



}
