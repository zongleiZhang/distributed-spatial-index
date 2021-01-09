package com.ada.common;

import com.ada.globalTree.GDataNode;
import com.ada.geometry.GridRectangle;
import com.ada.Hungarian.Hungary;
import com.ada.geometry.*;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Arrays;

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

    public static List<Integer> usedLeafID = new ArrayList<>();

    public static List<Integer> canUseLeafID = new ArrayList<>();


    /**
     * 密度统计的频度
     */
    public static int densityFre;

    /**
     * 全局索引做动态负载均衡的频度
     */
    public static int balanceFre;


    /**
     * 存放JobID的文件位置
     */
    private final static String jobIDFileName = "/opt/flink-1.9.1/log/flink-chenliang-standalonesession-0-131-199.log";



    /**
     * 全局索引叶节点索引项数量的下届
     */
    public static int globalLowBound;

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

    public final static Rectangle globalRegion = new Rectangle(new Point(0.0,0.0), new Point(8626.0,8872.0));


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
            globalLowBound = Integer.parseInt(pro.getProperty("globalLowBound"));
            windowSize = Integer.parseInt(pro.getProperty("windowSize"));
            logicWindow = Integer.parseInt(pro.getProperty("logicWindow"));
            topK = Integer.parseInt(pro.getProperty("topK"));
            KNum = Integer.parseInt(pro.getProperty("KNum"));
            extend = Double.parseDouble(pro.getProperty("extend"));
            t = Integer.parseInt(pro.getProperty("t"));
        }catch (Exception e){
            e.printStackTrace();
        }

        balanceFre = ((logicWindow/7)/densityFre)*densityFre;

        for (int i = dividePartition-1; i >= 0; i--)
            canUseLeafID.add(i);
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


    public static SimilarState getHausdorff(Trajectory t1, Trajectory t2){
        double[][] pds = pointDistance(t1, t2);
        Tuple2<Double, Integer>[] row = rowMin(pds);
        Tuple2<Double, Integer>[] col = colMin(pds);
        return new SimilarState(t1.TID, t2.TID, row, col);
    }



    public static void IOIOHausdorff(Trajectory t1,
                                     List<TrackPoint> inPoints1,
                                     Trajectory t2,
                                     List<TrackPoint> inPoints2,
                                     SimilarState state) {
        if (t1.TID != state.comparingTID){
            Trajectory tmpT = t1;
            t1 = t2;
            t2 = tmpT;
            List<TrackPoint> tmpP = inPoints1;
            inPoints1 = inPoints2;
            inPoints2 = tmpP;
        }
        int removeRowSize = state.row.length - t1.elms.size() + 1 - inPoints1.size();
        int removeColSize = state.col.length - t2.elms.size() + 1 - inPoints2.size();
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] row = new Tuple2[t1.elms.size() + 1];
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] col = new Tuple2[t2.elms.size() + 1];
        System.arraycopy(state.row, removeRowSize, row, 0, state.row.length - removeRowSize);
        System.arraycopy(state.col, removeColSize, col, 0, state.col.length - removeColSize);
        double[][] pdsRow = new double[inPoints1.size()][t2.elms.size()+1];
        double[][] pdsCol = new double[t1.elms.size()+1][inPoints2.size()];
        boolean[] rowFlag = new boolean[t1.elms.size() + 1];
        boolean[] colFlag = new boolean[t2.elms.size() + 1];
        for (int i = 0; i < state.row.length - removeRowSize; i++) {
            if ((row[i].f1 -= removeColSize) < 0){
                double[] pds = pointDistance(t1.getPoint(i), t2);
                rowFlag[i] = true;
                System.arraycopy(pds, state.row.length - removeRowSize, pdsCol[i], 0, inPoints2.size());
                row[i] = arrayMin(pds);
            }
        }
        for (int i = 0; i < state.col.length - removeColSize; i++) {
            if ((col[i].f1 -= removeRowSize) < 0){
                double[] pds = pointDistance(t2.getPoint(i), t1);
                colFlag[i] = true;
                for (int j = 0; j < inPoints1.size(); j++) pdsRow[j][i] = pds[j];
                col[i] = arrayMin(pds);
            }
        }
        for (int i = 0; i < inPoints1.size(); i++) {
            for (int j = 0; j < state.col.length - removeColSize; j++) {
                if (!colFlag[j]) {
                    double dis = t2.getPoint(j).distancePoint(inPoints1.get(i));
                    pdsRow[i][j] = dis;
                    if (dis < col[j].f0) {
                        col[j].f1 = state.row.length - removeRowSize + i;
                        col[j].f0 = dis;
                    }
                }
            }
        }
        for (int i = 0; i < state.row.length - removeRowSize; i++) {
            for (int j = 0; j < inPoints2.size(); j++) {
                if (!rowFlag[j]) {
                    double dis = t1.getPoint(i).distancePoint(inPoints2.get(j));
                    pdsCol[i][j] = dis;
                    if (dis < row[i].f0) {
                        row[i].f1 = state.col.length - removeColSize + j;
                        row[i].f0 = dis;
                    }
                }
            }
        }
        for (int i = 0; i < inPoints1.size(); i++) {
            for (int j = 0; j < inPoints2.size(); j++) {
                double dis = inPoints1.get(i).distancePoint(inPoints2.get(j));
                pdsCol[state.row.length - removeRowSize + i][j] = pdsRow[i][state.col.length - removeColSize + j] = dis;
            }
        }
        System.arraycopy(rowMin(pdsRow), 0, row, state.row.length - removeRowSize, inPoints1.size());
        System.arraycopy(colMin(pdsCol), 0, col, state.col.length - removeColSize, inPoints2.size());
        state.update(row, col);
    }

    public static void NOIOHausdorff(Trajectory t1,
                                     Trajectory t2,
                                     List<TrackPoint> inPoints2,
                                     SimilarState state) {
        if (t1.TID != state.comparingTID) {
            IONOHausdorff(t2, inPoints2, t1, state);
            return;
        }
    }

    public static void NNIOHausdorff(Trajectory t1,
                                     Trajectory t2,
                                     List<TrackPoint> inPoints2,
                                     SimilarState state) {

    }

    public static void INIOHausdorff(Trajectory t1,
                                     List<TrackPoint> inPoints1,
                                     Trajectory t2,
                                     List<TrackPoint> inPoints2,
                                     SimilarState state) {

    }

    public static void NONOHausdorff(Trajectory t1,
                                     Trajectory t2,
                                     SimilarState state) {
        if (t1.TID != state.comparingTID){
            Trajectory tmpT = t1;
            t1 = t2;
            t2 = tmpT;
        }
        int removeRowSize = state.row.length - t1.elms.size() + 1;
        int removeColSize = state.col.length - t2.elms.size() + 1;
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] row = new Tuple2[t1.elms.size() + 1];
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] col = new Tuple2[t2.elms.size() + 1];
        System.arraycopy(state.row, removeRowSize, row, 0, row.length);
        System.arraycopy(state.col, removeColSize, col, 0, col.length);
        for (int i = 0; i < row.length; i++) {
            if ((row[i].f1 -= removeColSize) < 0){
                row[i] = arrayMin(pointDistance(t1.getPoint(i), t2));
            }
        }
        for (int i = 0; i < col.length; i++) {
            if ((col[i].f1 -= removeRowSize) < 0){
                col[i] = arrayMin(pointDistance(t2.getPoint(i), t1));
            }
        }
        state.update(row, col);
    }

    public static void NNNOHausdorff(Trajectory t1,
                                     Trajectory t2,
                                     SimilarState state) {
        if (t1.TID != state.comparingTID) NONNHausdorff(t2, t1, state);
        int removeColSize = state.col.length - t2.elms.size() + 1;
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] col = new Tuple2[t2.elms.size() + 1];
        System.arraycopy(state.col, removeColSize, col, 0, col.length);
        for (int i = 0; i < state.row.length; i++) {
            if ((state.row[i].f1 -= removeColSize) < 0){
                state.row[i] = arrayMin(pointDistance(t1.getPoint(i), t2));
            }
        }
        state.update(state.row, col);

    }

    public static void INNOHausdorff(Trajectory t1,
                                     List<TrackPoint> inPoints1,
                                     Trajectory t2,
                                     SimilarState state) {
        if (t1.TID != state.comparingTID) NOINHausdorff(t2, t1, inPoints1, state);
    }

    public static void NNINHausdorff(Trajectory t1,
                                     Trajectory t2,
                                     List<TrackPoint> inPoints2,
                                     SimilarState state) {
        if (t1.TID != state.comparingTID){
            INNNHausdorff(t2, inPoints2, t1, state);
        }
        double[][] pdsCol = new double[t1.elms.size()+1][inPoints2.size()];
        for (int i = 0; i < state.row.length; i++) {
            for (int j = 0; j < inPoints2.size(); j++) {
                double dis = t1.getPoint(i).distancePoint(inPoints2.get(j));
                pdsCol[i][j] = dis;
                if (dis < state.row[i].f0){
                    state.row[i].f1 = state.col.length + j;
                    state.row[i].f0 = dis;
                }
            }
        }
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] col = new Tuple2[t2.elms.size()+1];
        System.arraycopy(state.col, 0, col, 0 , state.col.length);
        System.arraycopy(colMin(pdsCol), 0, col, state.col.length, inPoints2.size());
        state.update(state.row, col, getDistance(state.row, col));
    }

    public static void ININHausdorff(Trajectory t1,
                                     List<TrackPoint> inPoints1,
                                     Trajectory t2,
                                     List<TrackPoint> inPoints2,
                                     SimilarState state) {
        if (t1.TID != state.comparingTID){
            Trajectory tmpT = t1;
            t1 = t2;
            t2 = tmpT;
            List<TrackPoint> tmpP = inPoints1;
            inPoints1 = inPoints2;
            inPoints2 = tmpP;
        }
        double[][] pdsRow = new double[inPoints1.size()][t2.elms.size()+1];
        double[][] pdsCol = new double[t1.elms.size()+1][inPoints2.size()];
        for (int i = 0; i < inPoints1.size(); i++) {
            for (int j = 0; j < state.col.length; j++) {
                double dis = t2.getPoint(j).distancePoint(inPoints1.get(i));
                pdsRow[i][j] = dis;
                if (dis < state.col[j].f0){
                    state.col[j].f1 = state.row.length + i;
                    state.col[j].f0 = dis;
                }
            }
        }
        for (int i = 0; i < state.row.length; i++) {
            for (int j = 0; j < inPoints2.size(); j++) {
                double dis = t1.getPoint(i).distancePoint(inPoints2.get(j));
                pdsCol[i][j] = dis;
                if (dis < state.row[i].f0){
                    state.row[i].f1 = state.col.length + j;
                    state.row[i].f0 = dis;
                }
            }
        }
        for (int i = 0; i < inPoints1.size(); i++) {
            for (int j = 0; j < inPoints2.size(); j++) {
                double dis = inPoints1.get(i).distancePoint(inPoints2.get(j));
                pdsCol[state.row.length + i][j] = pdsRow[i][state.col.length + j] = dis;
            }
        }
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] row = new Tuple2[t1.elms.size()+1];
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] col = new Tuple2[t2.elms.size()+1];
        System.arraycopy(state.row, 0, row, 0 , state.row.length);
        System.arraycopy(state.col, 0, col, 0 , state.col.length);
        System.arraycopy(rowMin(pdsRow), 0, row, state.row.length, inPoints1.size());
        System.arraycopy(colMin(pdsCol), 0, col, state.col.length, inPoints2.size());
        state.update(row, col);
    }

    public static void NOINHausdorff(Trajectory t1, Trajectory t2, List<TrackPoint> inPoints2, SimilarState state) {
        if (t1.TID != state.comparingTID) INNOHausdorff(t2, inPoints2, t1, state);
    }

    public static void NONNHausdorff(Trajectory t1, Trajectory t2, SimilarState state) {
        if (t1.TID != state.comparingTID) NNNOHausdorff(t2, t1, state);
        int removeRowSize = state.row.length - t1.elms.size() + 1;
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] row = new Tuple2[t1.elms.size() + 1];
        System.arraycopy(state.row, removeRowSize, row, 0, row.length);
        for (int i = 0; i < state.col.length; i++) {
            if ((state.col[i].f1 -= removeRowSize) < 0){
                state.col[i] = arrayMin(pointDistance(t2.getPoint(i), t1));
            }
        }
        state.update(row, state.col);
    }

    public static void INNNHausdorff(Trajectory t1, List<TrackPoint> inPoints1, Trajectory t2, SimilarState state) {
        if (t1.TID != state.comparingTID) {
            NNINHausdorff(t2, t1, inPoints1, state);
        }
        double[][] pdsRow = new double[inPoints1.size()][t2.elms.size()+1];
        for (int i = 0; i < inPoints1.size(); i++) {
            for (int j = 0; j < state.col.length; j++) {
                double dis = t2.getPoint(j).distancePoint(inPoints1.get(i));
                pdsRow[i][j] = dis;
                if (dis < state.col[j].f0){
                    state.col[j].f1 = state.row.length + i;
                    state.col[j].f0 = dis;
                }
            }
        }
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] row = new Tuple2[t1.elms.size()+1];
        System.arraycopy(state.row, 0, row, 0 , state.row.length);
        System.arraycopy(rowMin(pdsRow), 0, row, state.row.length, inPoints1.size());
        state.update(row, state.col, getDistance(row, state.col));
    }

    public static void IONOHausdorff(Trajectory t1, List<TrackPoint> inPoints1, Trajectory t2, SimilarState state) {
        if (t1.TID != state.comparingTID) {
            NOIOHausdorff(t2, t1, inPoints1, state);
            return;
        }
    }

    public static void IOINHausdorff(Trajectory t1, List<TrackPoint> inPoints1, Trajectory t2, List<TrackPoint> inPoints2, SimilarState state) {
        if (t1.TID != state.comparingTID){
            INIOHausdorff(t2, inPoints2, t1, inPoints1, state);
            return;
        }
        int removeRowSize = state.row.length - t1.elms.size() + 1 - inPoints1.size();
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] row = new Tuple2[t1.elms.size() + 1];
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] col = new Tuple2[t2.elms.size() + 1];
        System.arraycopy(state.row, removeRowSize, row, 0, state.row.length - removeRowSize);
        System.arraycopy(state.col, 0, col, 0, state.col.length);
        double[][] pdsRow = new double[inPoints1.size()][t2.elms.size()+1];
        double[][] pdsCol = new double[t1.elms.size()+1][inPoints2.size()];
        boolean[] colFlag = new boolean[t2.elms.size() + 1];
        for (int i = 0; i < state.col.length; i++) {
            if ((col[i].f1 -= removeRowSize) < 0){
                double[] pds = pointDistance(t2.getPoint(i), t1);
                colFlag[i] = true;
                for (int j = 0; j < inPoints1.size(); j++) pdsRow[j][i] = pds[j];
                col[i] = arrayMin(pds);
            }
        }
        for (int i = 0; i < inPoints1.size(); i++) {
            for (int j = 0; j < state.col.length; j++) {
                if (!colFlag[j]) {
                    double dis = t2.getPoint(j).distancePoint(inPoints1.get(i));
                    pdsRow[i][j] = dis;
                    if (dis < col[j].f0) {
                        col[j].f1 = state.row.length - removeRowSize + i;
                        col[j].f0 = dis;
                    }
                }
            }
        }
        for (int i = 0; i < state.row.length - removeRowSize; i++) {
            for (int j = 0; j < inPoints2.size(); j++) {
                double dis = t1.getPoint(i).distancePoint(inPoints2.get(j));
                pdsCol[i][j] = dis;
                if (dis < row[i].f0) {
                    row[i].f1 = state.col.length + j;
                    row[i].f0 = dis;
                }
            }
        }
        for (int i = 0; i < inPoints1.size(); i++) {
            for (int j = 0; j < inPoints2.size(); j++) {
                double dis = inPoints1.get(i).distancePoint(inPoints2.get(j));
                pdsCol[state.row.length - removeRowSize + i][j] = pdsRow[i][state.col.length + j] = dis;
            }
        }
        System.arraycopy(rowMin(pdsRow), 0, row, state.row.length - removeRowSize, inPoints1.size());
        System.arraycopy(colMin(pdsCol), 0, col, state.col.length, inPoints2.size());
        state.update(row, col);
    }

    public static void IONNHausdorff(Trajectory t1, List<TrackPoint> inPoints1, Trajectory t2, SimilarState state) {
        NNIOHausdorff(t2, t1, inPoints1, state);
    }

    private static double getDistance(Tuple2<Double, Integer>[] t1, Tuple2<Double, Integer>[] t2){
        double distance = t1[0].f0;
        for (int i = 1; i < t1.length; i++)
            distance = Math.max(distance, t1[i].f0);
        for (Tuple2<Double, Integer> tuple2 : t2)
            distance = Math.max(distance, tuple2.f0);
        return distance;
    }

    /**
     * 求矩阵pds每一列的最小值。返回tuple2数组，数组的第i个元素，tuple.f0表示
     * pds第i列的最小值, tuple.f1表示这个最小值是哪一行的。
     */
    @SuppressWarnings("unchecked")
    private static Tuple2<Double, Integer>[] colMin(double[][] pds) {
        Tuple2<Double, Integer>[] res = new Tuple2[pds[0].length];
        for (int i = 0; i <res.length; i++) {
            int site = 0;
            double distance = pds[site][i];
            for (int j = 1; j < pds.length; j++) {
                if (distance > pds[j][i]){
                    distance = pds[j][i];
                    site = j;
                }
            }
            res[i] = new Tuple2<>(distance, site);
        }
        return res;
    }


    /**
     * 求矩阵pds每一行的最小值。返回tuple2数组，数组的第i个元素，tuple.f0表示
     * pds第i行的最小值, tuple.f1表示这个最小值是哪一列的。
     */
    @SuppressWarnings("unchecked")
    private static Tuple2<Double, Integer>[] rowMin(double[][] pds) {
        Tuple2<Double, Integer>[] res = new Tuple2[pds.length];
        for (int i = 0; i <res.length; i++)
            res[i] = arrayMin(pds[i]);
        return res;
    }

    private static Tuple2<Double, Integer> arrayMin(double[] pds) {
        int site = 0;
        double distance = pds[0];
        for (int i = 1; i < pds.length; i++) {
            if (distance > pds[i]){
                distance = pds[i];
                site = i;
            }
        }
        return new Tuple2<>(distance, site);
    }


    /**
     * 求两条轨迹t1和t2的每个采样点之前的距离，结果用二维数组返回。
     */
    private static double[][] pointDistance(Trajectory t1, Trajectory t2) {
        double[][] res = new double[t1.elms.size()+1][t2.elms.size()+1];
        int i = 0;
        for(Segment seg : t1.elms) res[i++] = pointDistance(seg.p1, t2);
        res[i] = pointDistance(t1.elms.getLast().p2, t2);
        return res;
    }


    /**
     * 求点p和轨迹t的每个采样点之前的距离，结果用数组返回。
     */
    private static double[] pointDistance(Point p, Trajectory t){
        double[] res = new double[t.elms.size()+1];
        int i = 0;
        for (Segment seg : t.elms) res[i++] = seg.p1.distancePoint(p);
        res[i] = t.elms.getLast().p2.distancePoint(p);
        return res;
    }

    private static void deDealColRow(Trajectory track0, Trajectory track1, List<Point> points0, Tuple2<Double, Integer>[] newRow) {
        int j = 0;
        for (Segment seg : track1.elms) {
            if (newRow[j].f1 < points0.size()){
                double[] ds = pointDistance(seg.p1, track0);
                newRow[j] = arrayMin(ds);
            }else {
                newRow[j].f1 -= points0.size();
            }
            j++;
        }
        if (newRow[j].f1 < points0.size()){
            double[] ds = pointDistance(track1.elms.getLast().p2, track0);
            newRow[j] = arrayMin(ds);
        }else {
            newRow[j].f1 -= points0.size();
        }
    }

    /**
     * 用动态规划计算DTW时，需要一个表格。本方法初始化一个表格
     * @param row 表格的行数
     * @param col 表格的列数
     * @return 表格
     */
    private static Tuple2<Double, Integer>[][] initPro( int row, int col){
        Tuple2<Double, Integer>[][] res = new Tuple2[row][];
        for (int i = 0; i < row; i++) {
            res[i] = new Tuple2[col];
            for (int j = 0; j < col; j++) {
                res[i][j] = new Tuple2<>(0.0, 0);
            }
        }
        for (Tuple2<Double, Integer>[] re : res)
            re[0].f0 = Double.MAX_VALUE;
        for (int k = 0; k < res[0].length; k++)
            res[0][k].f0 = Double.MAX_VALUE;
        res[0][0].f0 = 0.0;
        return res;
    }

    /**
     * 求DTW值
     * @param pds 点距离矩阵
     * @param pro DTW矩阵，全部赋值为0，矩阵大小比pds横竖都大1
     * @param i 开始行
     * @param j 开始列
     */
    private static void DTWValue(double[][] pds, Tuple2<Double, Integer>[][] pro, int i, int j) {
        if (i!=1 || j!=1) {
            for (int k = i; k < pro.length; k++) {
                for (int l = 1; l < j; l++)
                    computeRowColDTW(pds, pro, k, l);
            }
            for (int k = j; k < pro[0].length; k++) {
                for (int l = 1; l < i; l++)
                    computeRowColDTW(pds, pro, l, k);
            }
        }
        for (int k = i; k < pro.length; k++) {
            for (int l = j; l < pro[0].length; l++)
                computeRowColDTW(pds, pro, k, l);
        }
    }

    private static void computeRowColDTW(double[][] pds, Tuple2<Double, Integer>[][] res, int k, int l) {
        Tuple2<Double, Integer> min = min3(res[k - 1][l], res[k][l - 1], res[k - 1][l - 1]);
        res[k][l].f0 = pds[k - 1][l - 1] + min.f0;
        res[k][l].f1 = 1 + min.f1;
    }

    private static Tuple2<Double, Integer> min3(Tuple2<Double, Integer> d1, Tuple2<Double, Integer> d2, Tuple2<Double, Integer> d3){
        if (d1.f0 < d2.f0){
            if (d1.f0 < d3.f0)
                return d1;
            else if (d1.f0.equals(d3.f0)) {
                if (d1.f1 < d3.f1)
                    return d1;
                else
                    return d3;
            }else
                return d3;
        }else if(d1.f0.equals(d2.f0)){
            if (d3.f0 < d2.f0){
                return d3;
            }else if (d3.f0.equals(d2.f0)){
                if (d1.f1 < d2.f1) {
                    if (d1.f1 < d3.f1)
                        return d1;
                    else
                        return d3;
                }else {
                    if (d2.f1 < d3.f1)
                        return d2;
                    else
                        return d3;
                }
            }else {
                if (d1.f1 < d2.f1)
                    return d1;
                else
                    return d2;
            }
        }else{
            if (d2.f0 < d3.f0)
                return d2;
            else if (d2.f0.equals(d3.f0)) {
                if (d2.f1 < d3.f1)
                    return d2;
                else
                    return d3;
            }else
                return d3;
        }
    }

    /**
     * 向轨迹track中添加一个新的候选轨迹comparedTrack
     */
    public static void addTrackCandidate(TrackHauOne track, TrackHauOne comparedTrack) {
        SimilarState state = comparedTrack.getSimilarState(track.trajectory.TID);
        if (state == null) {
            state = Constants.getHausdorff(track.trajectory, comparedTrack.trajectory);
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
     * 缩减轨迹ID集合的元素数到 Constants.k*Constants.cDTW 大小
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

    public static Integer getLeafID(){
        if (canUseLeafID.isEmpty()){
            throw new IllegalArgumentException("LeafID is FPed");
        }else {
            Integer leafID = canUseLeafID.remove(canUseLeafID.size() - 1);
            usedLeafID.add(leafID);
            return leafID;
        }
    }

    public static void discardLeafID(Integer leafID){
        canUseLeafID.add(leafID);
        usedLeafID.remove(leafID);
    }



    public static String getJobIDStr() throws IOException {
        File f = new File(Constants.jobIDFileName);
        BufferedReader br = new BufferedReader(new FileReader(f));
        String jobIDStr = null;
        String str;
        while ( (str = br.readLine()) != null){
            int site = str.indexOf("Submitting job ");
            if (site != -1){
                str = str.substring(site);
                jobIDStr = str.split(" ")[2];
            }
        }
        return jobIDStr;
    }

    public static boolean isEqual(double a, double b){
        return Math.abs(a - b) < zero;
    }

    public static double roundDouble(double a){
        return (Math.round(a*1000000))/1000000.0;
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

    /**
     * 使用Hungarian Algorithm重新分配leafID.
     * @param matrix 分裂前后元素映射数量关系
     * @return 分配结果
     */
    public static int[][] redisPatchLeafID(int[][] matrix, int upBound){
        int rowNum = matrix.length;
        int colNum = matrix[0].length;
        int[][] newMatrix;
        if (rowNum > colNum){
            newMatrix = new int[rowNum][rowNum];
            for (int i = 0; i < newMatrix.length; i++) {
                for (int j = 0; j < newMatrix[0].length; j++) {
                    if (j >= matrix[0].length)
                        newMatrix[i][j] = upBound;
                    else
                        newMatrix[i][j] = upBound - matrix[i][j];
                }
            }
        }else {
            newMatrix = new int[colNum][];
            for (int i = 0; i < newMatrix.length; i++) {
                if (i < matrix.length) {
                    newMatrix[i] = new int[colNum];
                    for (int j = 0; j < colNum; j++)
                        newMatrix[i][j] = upBound - matrix[i][j];
                }else {
                    newMatrix[i] = new int[colNum];
                    Arrays.fill(newMatrix[i],upBound);
                }
            }
        }
        int[][] res = Hungary.calculate(newMatrix);
        List<Tuple2<Integer,Integer>> list = new ArrayList<>();
        for (int[] re : res)
            list.add(new Tuple2<>(re[0],re[1]));
        if (rowNum > colNum)
            list.removeIf(ints -> ints.f1 >= colNum);
        else
            list.removeIf(ints -> ints.f0 >= rowNum);
        res = new int[list.size()][2];
        for (int i = 0; i < res.length; i++) {
            res[i][0] = list.get(i).f0;
            res[i][1] = list.get(i).f1;
        }
        return res;
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
