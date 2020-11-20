package com.ada.common;

import com.ada.GlobalTree.GDataNode;
import com.ada.Grid.GridRectangle;
import com.ada.Hungarian.Hungary;
import com.ada.trackSimilar.*;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.assignKeyToParallelOperator;

public class Constants implements Serializable {

    /**
     * 定义Double类型的零
     */
    public final static double zero = 0.00001;

    public final static DecimalFormat df = new DecimalFormat("#.00000");

    public final static Random random = new Random(System.currentTimeMillis());

    /**
     * 全局索引的并行度
     */
    public static int globalPartition;

    /**
     * 本地索引的并行度
     */
    public static int dividePartition;

    public static int topicPartition;

    public static int keyTIDPartition;

    /**
     * subTask: globalSubTask
     * value: key
     */
    public static Map<Integer,Integer> globalSubTaskKayMap = new HashMap<>();

    /**
     * subTask: divideSubTask
     * value: key
     */
    public static Map<Integer,Integer> divideSubTaskKayMap = new HashMap<>();

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

    public final static String QueryStateIP = "192.168.131.199";
//    public final static String QueryStateIP = "localhost";

    /**
     * 存放JobID的文件位置
     */
//    private final static String jobIDFileName = "output.txt";
    private final static String jobIDFileName = "/opt/flink-1.9.1/log/flink-chenliang-standalonesession-0-131-199.log";
//    private final static String jobIDFileName = "F:\\softwares\\flink-1.9.1\\log\\flink-zonglei.zhang-jobmanager.log";

//    private final static String confFileName = "/home/chenliang/data/zzlDIC/conf.txt";
    private final static String confFileName = "conf.txt";


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

    public static String similar;

    public final static Rectangle globalRegion = new Rectangle(new Point(0.0,0.0), new Point(8626.0,8872.0));


    static {
        try {
            File f = new File(confFileName);
            BufferedReader br = new BufferedReader(new FileReader(f));
            Map<String,String> confMap = new HashMap<>();
            String line;
            while ((line = br.readLine())!=null){
                String[] split = line.split(":");
                confMap.put(split[0],split[1]);
            }
            topicPartition = Integer.parseInt(confMap.get("topicPartition"));
            globalPartition = Integer.parseInt(confMap.get("globalPartition"));
            dividePartition = Integer.parseInt(confMap.get("dividePartition"));
            keyTIDPartition = Integer.parseInt(confMap.get("keyTIDPartition"));
            densityFre = Integer.parseInt(confMap.get("densityFre"));
            globalLowBound = Integer.parseInt(confMap.get("globalLowBound"));
            windowSize = Integer.parseInt(confMap.get("windowSize"));
            logicWindow = Integer.parseInt(confMap.get("logicWindow"));
            topK = Integer.parseInt(confMap.get("topK"));
            KNum = Integer.parseInt(confMap.get("KNum"));
            similar = confMap.get("similar");
            extend = Double.parseDouble(confMap.get("extend"));
            t = Integer.parseInt(confMap.get("t"));
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
                globalSubTaskKayMap.put(subTask, i);
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
                divideSubTaskKayMap.put(subTask, i);
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

    public static SimilarState getDTW(Trajectory<TrackPointElem> t1, Trajectory<TrackPointElem> t2){
        double[][] pds = pointDistance(t1, t2);
        Tuple2<Double, Integer>[][] pro = initPro(t1.elems.size()+1,t2.elems.size()+1);
        for (int i = 1; i < pro.length; i++) {
            for (int i1 = 1; i1 < pro[i].length; i1++)
                pro[i][i1] = new Tuple2<>(0.0, 0);
        }
        DTWValue(pds,pro,1,1);
        Tuple2<Tuple2<Double, Integer>[], Tuple2<Double, Integer>[]> tuple2 = getDTWState(pro);
        return new SimilarState(t1.TID, t2.TID, tuple2.f0, tuple2.f1);
    }


    public static SimilarState getHausdorff(Trajectory<Segment> t1, Trajectory<Segment> t2){
        double[][] pds = pointDistance(t1, t2);
        Tuple2<Double, Integer>[] col = rowMin(pds);
        Tuple2<Double, Integer>[] row = colMin(pds);
        return new SimilarState(t1.TID, t2.TID, row, col);
    }



    /**
     * 轨迹的新的采样点points到达时，增量计算其余与轨迹trajectory的DTW距离，并将其计算的中间结果更新到state中。
     * @param points 新到达的采样点
     * @param trajectory 被计算相似度的轨迹
     * @param state 相似度计算中间状态
     */
    public static<T extends Point & TrackInfo> void incrementDTW(List<T> points, Trajectory<TrackPointElem> trajectory, SimilarState state){
        double[][] pds;
        if (state.comparingTID == points.get(0).obtainTID()){
            int oldRowNum = state.col.length;
            int newRowNum = state.col.length+points.size();
            int colNum = state.row.length;
            pds = new double[newRowNum][colNum];
            for (int k = oldRowNum; k < newRowNum; k++) {
                for (int l = 0; l < colNum; l++)
                    pds[k][l] = points.get(k - oldRowNum).distancePoint(trajectory.elems.get(l));
            }
            Tuple2<Double, Integer>[][] pro = initPro(newRowNum+1, colNum+1);
            System.arraycopy(state.row, 0, pro[oldRowNum], 1, colNum);
            for (int i = 0; i < oldRowNum; i++)
                pro[i+1][colNum] = state.col[i];
            DTWValue(pds,pro,oldRowNum+1,colNum+1);
            Tuple2<Tuple2<Double, Integer>[], Tuple2<Double, Integer>[]> tuple2 = getDTWState(pro);
            state.update(tuple2.f0,tuple2.f1);
        }else{
            int rowNum = state.col.length;
            int oldColNum = state.row.length;
            int newColNum = state.row.length + points.size();
            pds = new double[rowNum][newColNum];
            for (int k = oldColNum; k < newColNum; k++) {
                for (int l = 0; l < rowNum; l++)
                    pds[l][k] = points.get(k-oldColNum).distancePoint(trajectory.elems.get(l));
            }
            Tuple2<Double, Integer>[][] pro = initPro(rowNum+1, newColNum+1);
            System.arraycopy(state.row, 0, pro[rowNum], 1, oldColNum);
            for (int i = 0; i < rowNum; i++)
                pro[i+1][oldColNum] = state.col[i];
            DTWValue(pds,pro,rowNum+1,oldColNum+1);
            Tuple2<Tuple2<Double, Integer>[], Tuple2<Double, Integer>[]> tuple2 = getDTWState(pro);
            state.update(tuple2.f0,tuple2.f1);
        }
    }

    private static Tuple2<Tuple2<Double, Integer>[], Tuple2<Double, Integer>[]> getDTWState(Tuple2<Double, Integer>[][] pro){
        Tuple2<Double, Integer>[] row = new Tuple2[pro[0].length-1];
        Tuple2<Double, Integer>[] col = new Tuple2[pro.length-1];
        System.arraycopy(pro[pro.length - 1], 1, row, 0, row.length);
        for (int i = 0; i < col.length; i++)
            col[i] = pro[i+1][pro[0].length-1];
        return new Tuple2<>(row, col);
    }


    public static <T extends Point & TrackInfo> void incrementHausdorff(List<T> points, Trajectory<Segment> track, SimilarState state){
        double[][] pds;
        if (state.comparingTID == points.get(0).obtainTID()){
            int oldRowNum = state.col.length;
            int newRowNum = points.size();
            int colNum = state.row.length;
            pds = new double[newRowNum][colNum];
            for (int i = 0; i < pds.length; i++)
                pds[i] = pointDistance(points.get(i), track);
            Tuple2<Double, Integer>[] row = rowMin(pds);
            Tuple2<Double, Integer>[] col = colMin(pds);
            Tuple2<Double, Integer>[] newCol = new Tuple2[newRowNum + oldRowNum];
            System.arraycopy(state.col, 0, newCol, 0, state.col.length);
            System.arraycopy(row, 0, newCol, state.col.length, row.length);
            Tuple2<Double, Integer>[] newRow = new Tuple2[colNum];
            for (int i = 0; i < newRow.length; i++) {
                if (col[i].f0 < state.row[i].f0)
                    newRow[i] = new Tuple2<>(col[i].f0, col[i].f1 + state.col.length);
                else
                    newRow[i] = state.row[i];
            }
            state.update(newRow,newCol);
        }else{
            int oldColNum = state.row.length;
            int newColNum = points.size();
            int rowNum = state.col.length;
            pds = new double[rowNum][newColNum];
            int i = 0;
            for (Segment seg : track.elems) {
                for (int j = 0; j < points.size(); j++)
                    pds[i][j] = seg.p1.distancePoint(points.get(j));
                i++;
            }
            for (int j = 0; j < points.size(); j++)
                pds[i][j] = track.elems.getLast().p2.distancePoint(points.get(j));
            Tuple2<Double, Integer>[] row = rowMin(pds);
            Tuple2<Double, Integer>[] col = colMin(pds);
            Tuple2<Double, Integer>[] newRow = new Tuple2[newColNum + oldColNum];
            System.arraycopy(state.row, 0, newRow, 0, state.row.length);
            System.arraycopy(col, 0, newRow, state.row.length, col.length);
            Tuple2<Double, Integer>[] newCol = new Tuple2[rowNum];
            for (i = 0; i < newCol.length; i++) {
                if (row[i].f0 < state.col[i].f0)
                    newCol[i] = new Tuple2<>(row[i].f0, row[i].f1 + state.row.length);
                else
                    newCol[i] = state.col[i];
            }
            state.update(newRow,newCol);
        }
    }

    /**
     * 求矩阵pds每一列的最小值。返回tuple2数组，数组的第i个元素，tuple.f0表示
     * pds第i列的最小值, tuple.f1表示这个最小值是哪一行的。
     */
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
    private static <T extends TrackInfo> double[][] pointDistance(Trajectory<T> t1, Trajectory<T> t2) {
        if (t1.elems.getFirst() instanceof Segment){
            Trajectory<Segment> tt1 = (Trajectory<Segment>) t1;
            Trajectory<Segment> tt2 = (Trajectory<Segment>) t2;
            if (t1 == null || t2 == null || t1.elems == null || t2.elems == null)
                System.out.print("");
            double[][] res = new double[t1.elems.size()+1][t2.elems.size()+1];
            int i = 0;
            for(Segment seg : tt1.elems){
                res[i] = pointDistance(seg.p1, tt2);
                i++;
            }
            res[i] = pointDistance(tt1.elems.getLast().p2, tt2);
            return res;
        }else {
            Trajectory<TrackPointElem> tt1 = (Trajectory<TrackPointElem>) t1;
            Trajectory<TrackPointElem> tt2 = (Trajectory<TrackPointElem>) t2;
            double[][] res = new double[t1.elems.size()][t2.elems.size()];
            int i = 0;
            for(TrackPointElem point:tt1.elems){
                res[i] = pointDistance(point, tt2);
                i++;
            }
            return res;
        }

    }


    /**
     * 求点p和轨迹t的每个采样点之前的距离，结果用数组返回。
     */
    private static <T extends TrackInfo> double[] pointDistance(Point p, Trajectory<T> t){
        if (t.elems.getFirst() instanceof Segment){
            Trajectory<Segment> tt = (Trajectory<Segment>) t;
            double[] res = new double[tt.elems.size()+1];
            int i = 0;
            for (Segment seg : tt.elems) {
                res[i] = seg.p1.distancePoint(p);
                i++;
            }
            res[i] = tt.elems.getLast().p2.distancePoint(p);
            return res;
        }else {
            Trajectory<TrackPointElem> tt = (Trajectory<TrackPointElem>) t;
            double[] res = new double[tt.elems.size()];
            int i = 0;
            for (TrackPointElem pointElem : tt.elems) {
                res[i] = pointElem.distancePoint(p);
                i++;
            }
            return res;
        }
    }

    /**
     * 轨迹Hausdorff距离的减量计算。
     * @param track0 有采样点移出的轨迹
     * @param track1 无变化的轨迹
//     * @param points deTrack移出的采样点
     * @param state 轨迹相似度中间状态，该方法修改这个中间状态。
     */
    public static void decrementHausdorff(Trajectory<Segment> track0,
                                          List<Segment> segments0,
                                          Trajectory<Segment> track1,
                                          List<Segment> segments1,
                                          SimilarState state){
        Trajectory<Segment> tmpTrack;
        List<Segment> tmpPoints;
        if (track1.TID == state.comparingTID){
            tmpTrack = track0;
            track0 = track1;
            track1 = tmpTrack;
            tmpPoints = segments0;
            segments0 = segments1;
            segments1 = tmpPoints;
        }
        if (segments0 == null || segments1 == null)
            System.out.print("");
        List<Point> points0 = Segment.segmentsToPoints(segments0);
        List<Point> points1 = Segment.segmentsToPoints(segments1);
        if (state.row.length != track1.elems.size()+segments1.size()+1 ||
                state.col.length != track0.elems.size()+segments0.size()+1)
            throw new IllegalArgumentException(Constants.logicWindow + " " + "error decrement");
        Tuple2<Double, Integer>[] newRow = new Tuple2[track1.elems.size()+1];
        Tuple2<Double, Integer>[] newCol = new Tuple2[track0.elems.size()+1];
        System.arraycopy(state.row, segments1.size(), newRow, 0, newRow.length);
        System.arraycopy(state.col, segments0.size(), newCol, 0, newCol.length);
        deDealColRow(track1, track0, points1, newCol);
        deDealColRow(track0, track1, points0, newRow);
        state.update(newRow, newCol);
    }

    private static void deDealColRow(Trajectory<Segment> track0, Trajectory<Segment> track1, List<Point> points0, Tuple2<Double, Integer>[] newRow) {
        int j = 0;
        for (Segment seg : track1.elems) {
            if (newRow[j].f1 < points0.size()){
                double[] ds = pointDistance(seg.p1, track0);
                newRow[j] = arrayMin(ds);
            }else {
                newRow[j].f1 -= points0.size();
            }
            j++;
        }
        if (newRow[j].f1 < points0.size()){
            double[] ds = pointDistance(track1.elems.getLast().p2, track0);
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
    public static Rectangle getPruningRegion(Trajectory<Segment> track, double threshold){
        Rectangle rectangle = null;
        boolean flag = true;
        for (Segment s : track.elems) {
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


    /**
     * 两个同型矩阵a,b。将b中的每个元素加到（isAdd是true）或者去减（isAdd是false）a中的对应元素上。
     */
    public static void addArrsToArrs (int[][] a, int[][] b, boolean isAdd){
        if (isAdd){
            for (int i = 0; i < a.length; i++) {
                for (int j = 0; j < a[i].length; j++)
                    a[i][j] += b[i][j];
            }
        }else {
            for (int i = 0; i < a.length; i++) {
                for (int j = 0; j < a[i].length; j++)
                    a[i][j] -= b[i][j];
            }
        }

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
