package com.ada.common;

import com.ada.geometry.*;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

public class Hausdorff {

    public static SimilarState getHausdorff(Trajectory t1, Trajectory t2){
        double[][] pds = pointDistance(t1, t2);
        Tuple2<Double, Integer>[] row = rowMin(pds);
        Tuple2<Double, Integer>[] col = colMin(pds);
        return new SimilarState(t1.TID, t2.TID, row, col);
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
        int removeRowSize = state.row.length - ((t1.elms.size() + 1) - inPoints1.size());
        int removeColSize = state.col.length - ((t2.elms.size() + 1) - inPoints2.size());
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
                System.arraycopy(pds, state.col.length - removeColSize, pdsCol[i], 0, inPoints2.size());
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
        int removeRowSize = state.row.length - (t1.elms.size() + 1);
        int removeColSize = state.col.length - ((t2.elms.size() + 1) - inPoints2.size());
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] row = new Tuple2[t1.elms.size() + 1];
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] col = new Tuple2[t2.elms.size() + 1];
        System.arraycopy(state.row, removeRowSize, row, 0, state.row.length - removeRowSize);
        System.arraycopy(state.col, removeColSize, col, 0, state.col.length - removeColSize);
        double[][] pdsCol = new double[t1.elms.size()+1][inPoints2.size()];
        boolean[] rowFlag = new boolean[t1.elms.size() + 1];
        for (int i = 0; i < state.row.length - removeRowSize; i++) {
            if ((row[i].f1 -= removeColSize) < 0){
                double[] pds = pointDistance(t1.getPoint(i), t2);
                rowFlag[i] = true;
                System.arraycopy(pds, state.col.length - removeColSize, pdsCol[i], 0, inPoints2.size());
                row[i] = arrayMin(pds);
            }
        }
        for (int i = 0; i < state.col.length - removeColSize; i++) {
            if ((col[i].f1 -= removeRowSize) < 0) {
                col[i] = arrayMin(pointDistance(t2.getPoint(i), t1));
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
        System.arraycopy(colMin(pdsCol), 0, col, state.col.length - removeColSize, inPoints2.size());
        state.update(row, col);
    }

    public static void NNIOHausdorff(Trajectory t1,
                                     Trajectory t2,
                                     List<TrackPoint> inPoints2,
                                     SimilarState state) {
        if (t1.TID != state.comparingTID){
            IONNHausdorff(t2, inPoints2, t1, state);
            return;
        }
        int removeColSize = state.col.length - ((t2.elms.size() + 1) - inPoints2.size());
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] row = new Tuple2[t1.elms.size() + 1];
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] col = new Tuple2[t2.elms.size() + 1];
        System.arraycopy(state.row, 0, row, 0, state.row.length);
        System.arraycopy(state.col, removeColSize, col, 0, state.col.length - removeColSize);
        double[][] pdsCol = new double[t1.elms.size()+1][inPoints2.size()];
        boolean[] rowFlag = new boolean[t1.elms.size() + 1];
        for (int i = 0; i < state.row.length; i++) {
            if ((row[i].f1 -= removeColSize) < 0){
                double[] pds = pointDistance(t1.getPoint(i), t2);
                rowFlag[i] = true;
                System.arraycopy(pds, state.col.length - removeColSize, pdsCol[i], 0, inPoints2.size());
                row[i] = arrayMin(pds);
            }
        }
        for (int i = 0; i < state.row.length; i++) {
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
        System.arraycopy(colMin(pdsCol), 0, col, state.col.length - removeColSize, inPoints2.size());
        state.update(row, col);
    }

    public static void INIOHausdorff(Trajectory t1,
                                     List<TrackPoint> inPoints1,
                                     Trajectory t2,
                                     List<TrackPoint> inPoints2,
                                     SimilarState state) {
        if (t1.TID != state.comparingTID){
            IOINHausdorff(t2, inPoints2, t1, inPoints1, state);
            return;
        }
        int removeColSize = state.col.length - (t2.elms.size() + 1);
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] row = new Tuple2[t1.elms.size() + 1];
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] col = new Tuple2[t2.elms.size() + 1];
        System.arraycopy(state.row, 0, row, 0, state.row.length);
        System.arraycopy(state.col, removeColSize, col, 0, state.col.length - removeColSize);
        double[][] pdsRow = new double[inPoints1.size()][t2.elms.size()+1];
        double[][] pdsCol = new double[t1.elms.size()+1][inPoints2.size()];
        boolean[] rowFlag = new boolean[t1.elms.size() + 1];
        for (int i = 0; i < state.row.length; i++) {
            if ((row[i].f1 -= removeColSize) < 0){
                double[] pds = pointDistance(t1.getPoint(i), t2);
                rowFlag[i] = true;
                System.arraycopy(pds, state.col.length - removeColSize, pdsCol[i], 0, inPoints2.size());
                row[i] = arrayMin(pds);
            }
        }
        for (int i = 0; i < inPoints1.size(); i++) {
            for (int j = 0; j < state.col.length - removeColSize; j++) {
                double dis = t2.getPoint(j).distancePoint(inPoints1.get(i));
                pdsRow[i][j] = dis;
                if (dis < col[j].f0) {
                    col[j].f1 = state.row.length + i;
                    col[j].f0 = dis;
                }
            }
        }
        for (int i = 0; i < state.row.length; i++) {
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
                pdsCol[state.row.length + i][j] = pdsRow[i][state.col.length - removeColSize + j] = dis;
            }
        }
        System.arraycopy(rowMin(pdsRow), 0, row, state.row.length, inPoints1.size());
        System.arraycopy(colMin(pdsCol), 0, col, state.col.length - removeColSize, inPoints2.size());
        state.update(row, col);
    }

    public static void NONOHausdorff(Trajectory t1,
                                     Trajectory t2,
                                     SimilarState state) {
        if (t1.TID != state.comparingTID){
            Trajectory tmpT = t1;
            t1 = t2;
            t2 = tmpT;
        }
        int removeRowSize = state.row.length - (t1.elms.size() + 1);
        int removeColSize = state.col.length - (t2.elms.size() + 1);
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
        if (t1.TID != state.comparingTID) {
            NONNHausdorff(t2, t1, state);
            return;
        }
        int removeColSize = state.col.length - (t2.elms.size() + 1);
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
        if (t1.TID != state.comparingTID) {
            NOINHausdorff(t2, t1, inPoints1, state);
            return;
        }
        int removeColSize = state.col.length - (t2.elms.size() + 1);
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] row = new Tuple2[t1.elms.size() + 1];
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] col = new Tuple2[t2.elms.size() + 1];
        System.arraycopy(state.row, 0, row, 0, state.row.length);
        System.arraycopy(state.col, removeColSize, col, 0, state.col.length - removeColSize);
        double[][] pdsRow = new double[inPoints1.size()][t2.elms.size()+1];
        for (int i = 0; i < state.row.length; i++) {
            if ((row[i].f1 -= removeColSize) < 0){
                row[i] = arrayMin(pointDistance(t1.getPoint(i), t2));
            }
        }
        for (int i = 0; i < inPoints1.size(); i++) {
            for (int j = 0; j < state.col.length - removeColSize; j++) {
                double dis = t2.getPoint(j).distancePoint(inPoints1.get(i));
                pdsRow[i][j] = dis;
                if (dis < col[j].f0) {
                    col[j].f1 = state.row.length + i;
                    col[j].f0 = dis;
                }
            }
        }
        System.arraycopy(rowMin(pdsRow), 0, row, state.row.length, inPoints1.size());
        state.update(row, col);
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
        if (t1.TID != state.comparingTID) {
            INNOHausdorff(t2, inPoints2, t1, state);
            return;
        }
        int removeRowSize = state.row.length - (t1.elms.size() + 1);
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] row = new Tuple2[t1.elms.size() + 1];
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] col = new Tuple2[t2.elms.size() + 1];
        System.arraycopy(state.row, removeRowSize, row, 0, state.row.length - removeRowSize);
        System.arraycopy(state.col, 0, col, 0, state.col.length);
        double[][] pdsCol = new double[t1.elms.size()+1][inPoints2.size()];
        for (int i = 0; i < state.col.length; i++) {
            if ((col[i].f1 -= removeRowSize) < 0){
                col[i] = arrayMin(pointDistance(t2.getPoint(i), t1));
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
        System.arraycopy(colMin(pdsCol), 0, col, state.col.length, inPoints2.size());
        state.update(row, col);
    }

    public static void NONNHausdorff(Trajectory t1, Trajectory t2, SimilarState state) {
        if (t1.TID != state.comparingTID) {
            NNNOHausdorff(t2, t1, state);
            return;
        }
        int removeRowSize = state.row.length - (t1.elms.size() + 1);
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
        int removeRowSize = state.row.length - ((t1.elms.size() + 1) - inPoints1.size());
        int removeColSize = state.col.length - (t2.elms.size() + 1);
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] row = new Tuple2[t1.elms.size() + 1];
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] col = new Tuple2[t2.elms.size() + 1];
        System.arraycopy(state.row, removeRowSize, row, 0, state.row.length - removeRowSize);
        System.arraycopy(state.col, removeColSize, col, 0, state.col.length - removeColSize);
        double[][] pdsRow = new double[inPoints1.size()][t2.elms.size()+1];
        boolean[] colFlag = new boolean[t2.elms.size() + 1];
        for (int i = 0; i < state.row.length - removeRowSize; i++) {
            if ((row[i].f1 -= removeColSize) < 0){
                row[i] = arrayMin(pointDistance(t1.getPoint(i), t2));
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

        for (int j = 0; j < state.col.length - removeColSize; j++) {
            if (!colFlag[j]) {
                for (int i = 0; i < inPoints1.size(); i++) {
                    double dis = t2.getPoint(j).distancePoint(inPoints1.get(i));
                    pdsRow[i][j] = dis;
                    if (dis < col[j].f0) {
                        col[j].f1 = state.row.length - removeRowSize + i;
                        col[j].f0 = dis;
                    }
                }
            }
        }
        System.arraycopy(rowMin(pdsRow), 0, row, state.row.length - removeRowSize, inPoints1.size());
        state.update(row, col);
    }

    public static void IOINHausdorff(Trajectory t1, List<TrackPoint> inPoints1, Trajectory t2, List<TrackPoint> inPoints2, SimilarState state) {
        if (t1.TID != state.comparingTID){
            INIOHausdorff(t2, inPoints2, t1, inPoints1, state);
            return;
        }
        int removeRowSize = state.row.length - ((t1.elms.size() + 1) - inPoints1.size());
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
        if (t1.TID != state.comparingTID) {
            NNIOHausdorff(t2, t1, inPoints1, state);
            return;
        }
        int removeRowSize = state.row.length - ((t1.elms.size() + 1) - inPoints1.size());
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] row = new Tuple2[t1.elms.size() + 1];
        @SuppressWarnings("unchecked")
        Tuple2<Double, Integer>[] col = new Tuple2[t2.elms.size() + 1];
        System.arraycopy(state.row, removeRowSize, row, 0, state.row.length - removeRowSize);
        System.arraycopy(state.col, 0, col, 0, state.col.length);
        double[][] pdsRow = new double[inPoints1.size()][t2.elms.size()+1];
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
        System.arraycopy(rowMin(pdsRow), 0, row, state.row.length - removeRowSize, inPoints1.size());
        state.update(row, col);
    }

    private static double getDistance(Tuple2<Double, Integer>[] t1, Tuple2<Double, Integer>[] t2){
        double distance = t1[0].f0;
        for (int i = 1; i < t1.length; i++)
            distance = Math.max(distance, t1[i].f0);
        for (Tuple2<Double, Integer> tuple2 : t2)
            distance = Math.max(distance, tuple2.f0);
        return distance;
    }
}
