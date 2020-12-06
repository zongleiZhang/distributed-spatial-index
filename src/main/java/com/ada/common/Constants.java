package com.ada.common;

import com.ada.GlobalTree.GDataNode;
import com.ada.Grid.GridRectangle;
import com.ada.Hungarian.Hungary;
import com.ada.geometry.*;
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

    /**
     * 全局索引的并行度
     */
    public static int globalPartition;

    /**
     * 本地索引的并行度
     */
    public static int dividePartition;

    /**
     * kafka的topic的并行度
     */
    public static int topicPartition;

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

    public final static String QueryStateIP = "192.168.131.199";
//    public final static String QueryStateIP = "localhost";

    /**
     * 存放JobID的文件位置
     */
//    private final static String jobIDFileName = "output.txt";
//    private final static String jobIDFileName = "/opt/flink-1.9.1/log/flink-chenliang-standalonesession-0-131-199.log";
    private final static String jobIDFileName = "F:\\softwares\\flink-1.9.1\\log\\flink-zonglei.zhang-jobmanager.log";

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

    public static long windowSize;

    public static int logicWindow;

    public final static Rectangle globalRegion = new Rectangle(new Point(0.0,0.0), new Point(8626.0,8872.0));


    static {
        try {
            Properties pro = new Properties();
            FileInputStream in = new FileInputStream(confFileName);
            pro.load(in);
            in.close();
            topicPartition = Integer.parseInt(pro.getProperty("topicPartition"));



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
            densityFre = Integer.parseInt(confMap.get("densityFre"));
            globalLowBound = Integer.parseInt(confMap.get("globalLowBound"));
            windowSize = Integer.parseInt(confMap.get("windowSize"));
            logicWindow = Integer.parseInt(confMap.get("logicWindow"));
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


    /**
     * 判断segment是不是一个分布式空间索引的查询项
     */
    public static boolean isQuerySegmentItem(Segment segment){
        return segment.data == null &&
                segment.p1.timestamp > segment.p2.timestamp;
    }

}
