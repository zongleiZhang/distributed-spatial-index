package com.ada.Hausdorff;

import com.ada.common.ArrayQueue;
import com.ada.geometry.Point;
import com.ada.geometry.Segment;
import com.ada.geometry.TrackPoint;
import com.ada.geometry.Trajectory;

import java.util.List;

public class Hausdorff {

    public static SimilarState getHausdorff(Trajectory t1, Trajectory t2){
        double[][] pds = pointDistance(t1, t2);
        ArrayQueue<NOAndDistance> row = rowMin(pds);
        ArrayQueue<NOAndDistance> col = colMin(pds);
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
    private static ArrayQueue<NOAndDistance> rowMin(double[][] pds) {
        ArrayQueue<NOAndDistance> res = new ArrayQueue<>(pds.length);
        for (double[] pd : pds) res.add(arrayMin(pd));
        return res;
    }

    /**
     * 求矩阵pds每一列的最小值。返回tuple2数组，数组的第i个元素，tuple.f0表示
     * pds第i列的最小值, tuple.f1表示这个最小值是哪一行的。
     */
    private static ArrayQueue<NOAndDistance> colMin(double[][] pds) {
        ArrayQueue<NOAndDistance> res = new ArrayQueue<>(pds[0].length);
        for (int i = 0; i < pds[0].length; i++) {
            int site = 0;
            double distance = pds[site][i];
            for (int j = 1; j < pds.length; j++) {
                if (distance > pds[j][i]){
                    distance = pds[j][i];
                    site = j;
                }
            }
            res.add(new NOAndDistance(site, distance));
        }
        return res;
    }

    private static NOAndDistance arrayMin(double[] pds) {
        int site = 0;
        double distance = pds[0];
        for (int i = 1; i < pds.length; i++) {
            if (distance > pds[i]){
                distance = pds[i];
                site = i;
            }
        }
        return new NOAndDistance(site, distance);
    }

    private static void delPointsUpdateStateRow(Trajectory t1,
                                                Trajectory t2,
                                                List<TrackPoint> inPoints2,
                                                SimilarState state,
                                                int removeColSize,
                                                double[][] pdsCol,
                                                boolean[] rowFlag) {
        for (int i = 0; i < state.row.size(); i++) {
            if ((state.row.get(i).NO -= removeColSize) < 0) {
                double[] pds = pointDistance(t1.getPoint(i), t2);
                rowFlag[i] = true;
                System.arraycopy(pds, state.col.size(), pdsCol[i], 0, inPoints2.size());
                state.row.set(i, arrayMin(pds));
            }
        }
    }

    private static void delPointsUpdateStateCol(Trajectory t1,
                                                List<TrackPoint> inPoints1,
                                                Trajectory t2,
                                                SimilarState state,
                                                int removeRowSize,
                                                double[][] pdsRow,
                                                boolean[] colFlag) {
        for (int i = 0; i < state.col.size(); i++) {
            if ((state.col.get(i).NO -= removeRowSize) < 0) {
                double[] pds = pointDistance(t2.getPoint(i), t1);
                colFlag[i] = true;
                for (int j = 0; j < inPoints1.size(); j++) pdsRow[j][i] = pds[j];
                state.col.set(i, arrayMin(pds));
            }
        }
    }

    private static void addPointsUpdateStateCol(List<TrackPoint> inPoints1,
                                                Trajectory t2,
                                                SimilarState state,
                                                double[][] pdsRow,
                                                boolean[] colFlag) {
        for (int i = 0; i < state.col.size(); i++) {
            if (!colFlag[i]) {
                NOAndDistance nd = state.col.get(i);
                for (int j = 0; j < inPoints1.size(); j++) {
                    double dis = t2.getPoint(i).distancePoint(inPoints1.get(j));
                    pdsRow[j][i] = dis;
                    if (dis < nd.distance) {
                        nd.setDistance(dis);
                        nd.setNO(state.row.size() + j);
                    }
                }
            }
        }
    }

    private static void addPointsUpdateStateRow(Trajectory t1,
                                                List<TrackPoint> inPoints2,
                                                SimilarState state,
                                                double[][] pdsCol,
                                                boolean[] rowFlag) {
        for (int i = 0; i < state.row.size(); i++) {
            if (!rowFlag[i]) {
                NOAndDistance nd = state.row.get(i);
                for (int j = 0; j < inPoints2.size(); j++) {
                    double dis = t1.getPoint(i).distancePoint(inPoints2.get(j));
                    pdsCol[i][j] = dis;
                    if (dis < nd.distance) {
                        nd.setNO(state.col.size() + j);
                        nd.setDistance(dis);
                    }
                }
            }
        }
    }

    private static void addPdsPoints(List<TrackPoint> inPoints1,
                                     List<TrackPoint> inPoints2,
                                     SimilarState state,
                                     double[][] pdsRow,
                                     double[][] pdsCol) {
        for (int i = 0; i < inPoints1.size(); i++) {
            for (int j = 0; j < inPoints2.size(); j++) {
                double dis = inPoints1.get(i).distancePoint(inPoints2.get(j));
                pdsCol[state.row.size() + i][j] = pdsRow[i][state.col.size() + j] = dis;
            }
        }
    }

    private static void addPointsUpdateStateCol(List<TrackPoint> inPoints1,
                                                Trajectory t2,
                                                SimilarState state,
                                                double[][] pdsRow) {
        for (int i = 0; i < state.col.size(); i++) {
            NOAndDistance nd = state.col.get(i);
            for (int j = 0; j < inPoints1.size(); j++) {
                double dis = t2.getPoint(i).distancePoint(inPoints1.get(j));
                pdsRow[j][i] = dis;
                if (dis < nd.distance) {
                    nd.setDistance(dis);
                    nd.setNO(state.row.size() + j);
                }
            }
        }
    }

    private static void addPointsUpdateStateRow(Trajectory t1,
                                                List<TrackPoint> inPoints2,
                                                SimilarState state,
                                                double[][] pdsCol) {
        for (int i = 0; i < state.row.size(); i++) {
            NOAndDistance nd = state.row.get(i);
            for (int j = 0; j < inPoints2.size(); j++) {
                double dis = t1.getPoint(i).distancePoint(inPoints2.get(j));
                pdsCol[i][j] = dis;
                if (dis < nd.distance) {
                    nd.setNO(state.col.size() + j);
                    nd.setDistance(dis);
                }
            }
        }
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
        int removeRowSize = state.row.size() - ((t1.elms.size() + 1) - inPoints1.size());
        int removeColSize = state.col.size() - ((t2.elms.size() + 1) - inPoints2.size());
        state.row.removeFirstN(removeRowSize);
        state.col.removeFirstN(removeColSize);
        double[][] pdsRow = new double[inPoints1.size()][t2.elms.size()+1];
        double[][] pdsCol = new double[t1.elms.size()+1][inPoints2.size()];
        boolean[] rowFlag = new boolean[t1.elms.size() + 1];
        boolean[] colFlag = new boolean[t2.elms.size() + 1];
        delPointsUpdateStateRow(t1, t2, inPoints2, state, removeColSize, pdsCol, rowFlag);
        delPointsUpdateStateCol(t1, inPoints1, t2, state, removeRowSize, pdsRow, colFlag);
        addPointsUpdateStateCol(inPoints1, t2, state, pdsRow, colFlag);
        addPointsUpdateStateRow(t1, inPoints2, state, pdsCol, rowFlag);
        addPdsPoints(inPoints1, inPoints2, state, pdsRow, pdsCol);
        state.row.addAll(rowMin(pdsRow));
        state.col.addAll(colMin(pdsCol));
        state.setDistance();
        if (!SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state))
            SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state);
    }

    public static void NOIOHausdorff(Trajectory t1,
                                     Trajectory t2,
                                     List<TrackPoint> inPoints2,
                                     SimilarState state) {
        if (t1.TID != state.comparingTID) {
            IONOHausdorff(t2, inPoints2, t1, state);
            return;
        }
        int removeRowSize = state.row.size() - (t1.elms.size() + 1);
        int removeColSize = state.col.size() - ((t2.elms.size() + 1) - inPoints2.size());
        state.row.removeFirstN(removeRowSize);
        state.col.removeFirstN(removeColSize);
        double[][] pdsCol = new double[t1.elms.size()+1][inPoints2.size()];
        boolean[] rowFlag = new boolean[t1.elms.size() + 1];
        delPointsUpdateStateRow(t1, t2, inPoints2, state, removeColSize, pdsCol, rowFlag);
        for (int i = 0; i < state.col.size(); i++) {
            if ((state.col.get(i).NO -= removeRowSize) < 0) {
                state.col.set(i, arrayMin(pointDistance(t2.getPoint(i), t1)));
            }
        }
        addPointsUpdateStateRow(t1, inPoints2, state, pdsCol, rowFlag);
        state.col.addAll(colMin(pdsCol));
        state.setDistance();
        if (!SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state))
            SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state);
    }

    public static void NNIOHausdorff(Trajectory t1,
                                     Trajectory t2,
                                     List<TrackPoint> inPoints2,
                                     SimilarState state) {
        if (t1.TID != state.comparingTID){
            IONNHausdorff(t2, inPoints2, t1, state);
            return;
        }
        int removeColSize = state.col.size() - ((t2.elms.size() + 1) - inPoints2.size());
        state.col.removeFirstN(removeColSize);
        double[][] pdsCol = new double[t1.elms.size()+1][inPoints2.size()];
        boolean[] rowFlag = new boolean[t1.elms.size() + 1];
        delPointsUpdateStateRow(t1, t2, inPoints2, state, removeColSize, pdsCol, rowFlag);
        addPointsUpdateStateRow(t1, inPoints2, state, pdsCol, rowFlag);
        state.col.addAll(colMin(pdsCol));
        state.setDistance();
        if (!SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state))
            SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state);
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
        int removeColSize = state.col.size() - ((t2.elms.size() + 1) - inPoints2.size());
        state.col.removeFirstN(removeColSize);
        double[][] pdsRow = new double[inPoints1.size()][t2.elms.size()+1];
        double[][] pdsCol = new double[t1.elms.size()+1][inPoints2.size()];
        boolean[] rowFlag = new boolean[t1.elms.size() + 1];
        delPointsUpdateStateRow(t1, t2, inPoints2, state, removeColSize, pdsCol, rowFlag);
        addPointsUpdateStateCol(inPoints1, t2, state, pdsRow);
        addPointsUpdateStateRow(t1, inPoints2, state, pdsCol, rowFlag);
        addPdsPoints(inPoints1, inPoints2, state, pdsRow, pdsCol);
        state.row.addAll(rowMin(pdsRow));
        state.col.addAll(colMin(pdsCol));
        state.setDistance();
        if (!SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state))
            SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state);
    }

    public static void NONOHausdorff(Trajectory t1,
                                     Trajectory t2,
                                     SimilarState state) {
        if (t1.TID != state.comparingTID){
            Trajectory tmpT = t1;
            t1 = t2;
            t2 = tmpT;
        }
        int removeRowSize = state.row.size() - (t1.elms.size() + 1);
        int removeColSize = state.col.size() - (t2.elms.size() + 1);
        state.row.removeFirstN(removeRowSize);
        state.col.removeFirstN(removeColSize);
        for (int i = 0; i < state.row.size(); i++) {
            if ((state.row.get(i).NO -= removeColSize) < 0){
                state.row.set(i, arrayMin(pointDistance(t1.getPoint(i), t2)));
            }
        }
        for (int i = 0; i < state.col.size(); i++) {
            if ((state.col.get(i).NO -= removeRowSize) < 0){
                state.col.set(i, arrayMin(pointDistance(t2.getPoint(i), t1)));
            }
        }
        state.setDistance();
        if (!SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state))
            SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state);
    }

    public static void NNNOHausdorff(Trajectory t1,
                                     Trajectory t2,
                                     SimilarState state) {
        if (t1.TID != state.comparingTID) {
            NONNHausdorff(t2, t1, state);
            return;
        }
        int removeColSize = state.col.size() - (t2.elms.size() + 1);
        state.col.removeFirstN(removeColSize);
        for (int i = 0; i < state.row.size(); i++) {
            if ((state.row.get(i).NO -= removeColSize) < 0){
                state.row.set(i, arrayMin(pointDistance(t1.getPoint(i), t2)));
            }
        }
        state.setDistance();
        if (!SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state))
            SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state);
    }

    public static void INNOHausdorff(Trajectory t1,
                                     List<TrackPoint> inPoints1,
                                     Trajectory t2,
                                     SimilarState state) {
        if (t1.TID != state.comparingTID) {
            NOINHausdorff(t2, t1, inPoints1, state);
            return;
        }
        if (t1.TID == 8 && t2.TID == 21801)
            System.out.print("");
        int removeColSize = state.col.size() - (t2.elms.size() + 1);
        state.col.removeFirstN(removeColSize);
        double[][] pdsRow = new double[inPoints1.size()][t2.elms.size()+1];
        for (int i = 0; i < state.row.size(); i++) {
            if ((state.row.get(i).NO -= removeColSize) < 0){
                state.row.set(i, arrayMin(pointDistance(t1.getPoint(i), t2)));
            }
        }
        addPointsUpdateStateCol(inPoints1, t2, state, pdsRow);
        state.row.addAll(rowMin(pdsRow));
        state.setDistance();
        if (!SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state))
            SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state);
    }

    public static void NNINHausdorff(Trajectory t1,
                                     Trajectory t2,
                                     List<TrackPoint> inPoints2,
                                     SimilarState state) {
        if (t1.TID != state.comparingTID){
            INNNHausdorff(t2, inPoints2, t1, state);
            return;
        }
        double[][] pdsCol = new double[t1.elms.size()+1][inPoints2.size()];
        addPointsUpdateStateRow(t1, inPoints2, state, pdsCol);
        state.col.addAll(colMin(pdsCol));
        state.setDistance();
        if (!SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state))
            SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state);
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
        addPointsUpdateStateCol(inPoints1, t2, state, pdsRow);
        addPointsUpdateStateRow(t1, inPoints2, state, pdsCol);
        addPdsPoints(inPoints1, inPoints2, state, pdsRow, pdsCol);
        state.row.addAll(rowMin(pdsRow));
        state.col.addAll(colMin(pdsCol));
        state.setDistance();
        if (!SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state))
            SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state);
    }

    public static void NOINHausdorff(Trajectory t1, Trajectory t2, List<TrackPoint> inPoints2, SimilarState state) {
        if (t1.TID != state.comparingTID) {
            INNOHausdorff(t2, inPoints2, t1, state);
            return;
        }
        int removeRowSize = state.row.size() - (t1.elms.size() + 1);
        state.row.removeFirstN(removeRowSize);
        double[][] pdsCol = new double[t1.elms.size()+1][inPoints2.size()];
        for (int i = 0; i < state.col.size(); i++) {
            if ((state.col.get(i).NO -= removeRowSize) < 0){
                state.col.set(i, arrayMin(pointDistance(t2.getPoint(i), t1)));
            }
        }
        addPointsUpdateStateRow(t1, inPoints2, state, pdsCol);
        state.col.addAll(colMin(pdsCol));
        state.setDistance();
        if (!SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state))
            SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state);
    }

    public static void NONNHausdorff(Trajectory t1, Trajectory t2, SimilarState state) {
        if (t1.TID != state.comparingTID) {
            NNNOHausdorff(t2, t1, state);
            return;
        }
        int removeRowSize = state.row.size() - (t1.elms.size() + 1);
        state.row.removeFirstN(removeRowSize);
        for (int i = 0; i < state.col.size(); i++) {
            if ((state.col.get(i).NO -= removeRowSize) < 0){
                state.col.set(i, arrayMin(pointDistance(t2.getPoint(i), t1)));
            }
        }
        state.setDistance();
        if (!SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state))
            SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state);
    }

    public static void INNNHausdorff(Trajectory t1, List<TrackPoint> inPoints1, Trajectory t2, SimilarState state) {
        if (t1.TID != state.comparingTID) {
            NNINHausdorff(t2, t1, inPoints1, state);
            return;
        }
        double[][] pdsRow = new double[inPoints1.size()][t2.elms.size()+1];
        addPointsUpdateStateCol(inPoints1, t2, state, pdsRow);
        state.row.addAll(rowMin(pdsRow));
        state.setDistance();
        if (!SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state))
            SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state);
    }

    public static void IONOHausdorff(Trajectory t1, List<TrackPoint> inPoints1, Trajectory t2, SimilarState state) {
        if (t1.TID != state.comparingTID) {
            NOIOHausdorff(t2, t1, inPoints1, state);
            return;
        }
        int removeRowSize = state.row.size() - ((t1.elms.size() + 1) - inPoints1.size());
        int removeColSize = state.col.size() - (t2.elms.size() + 1);
        state.row.removeFirstN(removeRowSize);
        state.col.removeFirstN(removeColSize);
        double[][] pdsRow = new double[inPoints1.size()][t2.elms.size()+1];
        boolean[] colFlag = new boolean[t2.elms.size() + 1];
        for (int i = 0; i < state.row.size(); i++) {
            if ((state.row.get(i).NO -= removeColSize) < 0){
                state.row.set(i, arrayMin(pointDistance(t1.getPoint(i), t2)));
            }
        }
        delPointsUpdateStateCol(t1, inPoints1, t2, state, removeRowSize, pdsRow, colFlag);

        addPointsUpdateStateCol(inPoints1, t2, state, pdsRow, colFlag);
        state.row.addAll(rowMin(pdsRow));
        state.setDistance();
        if (!SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state))
            SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state);
    }

    public static void IOINHausdorff(Trajectory t1, List<TrackPoint> inPoints1, Trajectory t2, List<TrackPoint> inPoints2, SimilarState state) {
        if (t1.TID != state.comparingTID){
            INIOHausdorff(t2, inPoints2, t1, inPoints1, state);
            return;
        }
        if (t1.TID == 9662 && t2.TID == 3710) //row 143 !=
            System.out.print("");
        int removeRowSize = state.row.size() - ((t1.elms.size() + 1) - inPoints1.size());
        state.row.removeFirstN(removeRowSize);
        double[][] pdsRow = new double[inPoints1.size()][t2.elms.size()+1];
        double[][] pdsCol = new double[t1.elms.size()+1][inPoints2.size()];
        boolean[] colFlag = new boolean[t2.elms.size() + 1];
        delPointsUpdateStateCol(t1, inPoints1, t2, state, removeRowSize, pdsRow, colFlag);
        addPointsUpdateStateCol(inPoints1, t2, state, pdsRow, colFlag);
        addPointsUpdateStateRow(t1, inPoints2, state, pdsCol);
        addPdsPoints(inPoints1, inPoints2, state, pdsRow, pdsCol);
        state.row.addAll(rowMin(pdsRow));
        state.col.addAll(colMin(pdsCol));
        state.setDistance();
        if (!SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state))
            SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state);
    }

    public static void IONNHausdorff(Trajectory t1, List<TrackPoint> inPoints1, Trajectory t2, SimilarState state) {
        if (t1.TID != state.comparingTID) {
            NNIOHausdorff(t2, t1, inPoints1, state);
            return;
        }
        int removeRowSize = state.row.size() - ((t1.elms.size() + 1) - inPoints1.size());
        state.row.removeFirstN(removeRowSize);
        double[][] pdsRow = new double[inPoints1.size()][t2.elms.size()+1];
        boolean[] colFlag = new boolean[t2.elms.size() + 1];
        delPointsUpdateStateCol(t1, inPoints1, t2, state, removeRowSize, pdsRow, colFlag);
        addPointsUpdateStateCol(inPoints1, t2, state, pdsRow, colFlag);
        state.row.addAll(rowMin(pdsRow));
        state.setDistance();
        if (!SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state))
            SimilarState.isEquals(Hausdorff.getHausdorff(t1, t2), state);
    }
}
