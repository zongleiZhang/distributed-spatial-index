package com.ada.common;

import com.ada.geometry.*;
import com.ada.globalTree.GTree;

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
     * 本系统要求轨迹段断电在坐标轴上的映射的最大离
     */
    public final static double maxSegment = 400.0;

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
    public static int inputPartition;

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
     * 全局索引做动态负载均衡的频度
     */
    public static int balanceFre;

    private final static String confFileName = "conf.properties";

    /**
     * 网格密度
     */
    public static int gridDensity;

    public static long windowSize;

    public static int logicWindow;

    //查询矩形的大小
    public static double radius;

    //查询和更新的比例
    public static int ratio;

    public final static Rectangle globalRegion = new Rectangle(new Point(0.0,0.0), new Point(8626.0,8872.0));

    static {
        try {
            Properties pro = new Properties();
            FileInputStream in = new FileInputStream(confFileName);
            pro.load(in);
            in.close();
            inputPartition = Integer.parseInt(pro.getProperty("inputPartition"));
            globalPartition = Integer.parseInt(pro.getProperty("globalPartition"));
            dividePartition = Integer.parseInt(pro.getProperty("dividePartition"));
            GTree.globalLowBound = Integer.parseInt(pro.getProperty("globalLowBound"));
            windowSize = Integer.parseInt(pro.getProperty("windowSize"));
            logicWindow = Integer.parseInt(pro.getProperty("logicWindow"));
            gridDensity = Integer.parseInt(pro.getProperty("gridDensity"));
            radius = Double.parseDouble(pro.getProperty("radius"));
            ratio = Integer.parseInt(pro.getProperty("ratio"));
        }catch (Exception e){
            e.printStackTrace();
        }
        balanceFre = logicWindow/6;

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

        /*
         * 86-- 128是 256
         */
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


    public static boolean isEqual(double a, double b){
        return Math.abs(a - b) < zero;
    }

}
